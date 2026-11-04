#include "rapidgzip_adapter.h"

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstring>
#include <exception>
#include <memory>
#include <stdexcept>
#include <string>
#include <utility>
#include <vector>

#include <filereader/Standard.hpp>
#include <rapidgzip/ParallelGzipReader.hpp>
#include <rapidgzip/gzip/GzipReader.hpp>

namespace
{
const std::size_t kReaderBlockSize = 4U * 1024U * 1024U;
const std::size_t kMaximumDecodedChunk = 1U * 1024U * 1024U;

class Cancelled : public std::exception
{
public:
	const char *what() const throw()
	{
		return "rapidgzip decoder cancelled";
	}
};

struct Span
{
	const unsigned char *data;
	std::size_t size;
};

struct DecodedChunk
{
	/* Each callback span is kept alive by one owner in storage. */
	std::vector<std::shared_ptr<const void> > storage;
	std::vector<Span> spans;
	std::size_t span_index;
	std::size_t span_offset;

	DecodedChunk()
		: storage(), spans(), span_index(0), span_offset(0)
	{
	}
};

void write_error(char *buffer, std::size_t buffer_size, const std::string &message)
{
	if (buffer == NULL || buffer_size == 0)
	{
		return;
	}
	const std::size_t count = std::min(buffer_size - 1, message.size());
	std::memcpy(buffer, message.data(), count);
	buffer[count] = '\0';
}

std::string exception_message(const std::exception &exception, const std::string &path)
{
	std::string message = "error decoding gzip input " + path;
	if (exception.what() != NULL && exception.what()[0] != '\0')
	{
		message += ": ";
		message += exception.what();
	}
	return message;
}

std::size_t span_bytes(const DecodedChunk &chunk)
{
	std::size_t total = 0;
	for (std::size_t i = 0; i < chunk.spans.size(); ++i)
	{
		total += chunk.spans[i].size;
	}
	return total;
}
}

struct SoapnukeRapidGzip
{
	std::unique_ptr<rapidgzip::ParallelGzipReader<> > reader;
	std::unique_ptr<rapidgzip::GzipReader> serial_reader;
	DecodedChunk chunk;
	std::atomic<bool> cancelled;
	bool finished;
	std::string error;
	std::string path;
	std::size_t decoder_threads;
	std::size_t decoded_bytes;

	SoapnukeRapidGzip()
		: reader(), serial_reader(), chunk(), cancelled(false), finished(false), error(), path(),
		  decoder_threads(0), decoded_bytes(0)
	{
	}

	bool has_data() const
	{
		return chunk.span_index < chunk.spans.size();
	}

	std::size_t span_bytes_without_prefix(const DecodedChunk &target, std::size_t first) const
	{
		std::size_t total = 0;
		for (std::size_t i = first; i < target.spans.size(); ++i)
		{
			total += target.spans[i].size;
		}
		return total;
	}

	void append_parallel_chunk(
		const std::shared_ptr<rapidgzip::ChunkData> &chunk_data,
		std::size_t offset_in_chunk,
		std::size_t data_size,
		DecodedChunk &target)
	{
		if (cancelled.load() || !chunk_data || data_size == 0)
		{
			if (cancelled.load())
			{
				throw Cancelled();
			}
			return;
		}
		target.storage.push_back(std::shared_ptr<const void>(
			chunk_data, static_cast<const void *>(chunk_data.get())));
		using rapidgzip::deflate::DecodedData;
		const std::size_t before = target.spans.size();
		for (DecodedData::Iterator it(*chunk_data, offset_in_chunk, data_size);
			static_cast<bool>(it); ++it)
		{
			const std::pair<const void *, std::size_t> span = *it;
			Span decoded;
			decoded.data = reinterpret_cast<const unsigned char *>(span.first);
			decoded.size = span.second;
			target.spans.push_back(decoded);
		}
		if (span_bytes_without_prefix(target, before) != data_size)
		{
			throw std::runtime_error("rapidgzip decoded span accounting mismatch");
		}
	}

	void append_serial_chunk(const void *data, std::uint64_t data_size, DecodedChunk &target)
	{
		if (cancelled.load())
		{
			throw Cancelled();
		}
		if (data_size == 0)
		{
			return;
		}
		std::shared_ptr<std::vector<unsigned char> > storage(
			new std::vector<unsigned char>(
				reinterpret_cast<const unsigned char *>(data),
				reinterpret_cast<const unsigned char *>(data) + data_size));
		target.storage.push_back(std::shared_ptr<const void>(
			storage, static_cast<const void *>(storage->data())));
		Span span;
		span.data = storage->data();
		span.size = static_cast<std::size_t>(data_size);
		target.spans.push_back(span);
	}

	int available(const unsigned char **data, std::size_t *size, std::string &message)
	{
		if (data == NULL || size == NULL)
		{
			message = "rapidgzip available call received a null output";
			return -1;
		}
		if (has_data())
		{
			const Span &span = chunk.spans[chunk.span_index];
			*data = span.data + chunk.span_offset;
			*size = span.size - chunk.span_offset;
			return 1;
		}
		if (!error.empty())
		{
			message = error;
			return -1;
		}
		*data = NULL;
		*size = 0;
		/* The caller must invoke refill. This is a synchronous refill request,
		 * not a report that a background worker is running. */
		return finished ? 0 : 2;
	}

	int refill(std::string &message)
	{
		if (has_data())
		{
			return 1;
		}
		if (cancelled.load() || finished)
		{
			finished = true;
			return 0;
		}

		DecodedChunk next;
		try
		{
			std::size_t decoded = 0;
			if (reader)
			{
				const typename rapidgzip::ParallelGzipReader<>::WriteFunctor write_functor =
					[this, &next](
						const std::shared_ptr<rapidgzip::ChunkData> &chunk_data,
						std::size_t offset_in_chunk,
						std::size_t data_size) {
						append_parallel_chunk(chunk_data, offset_in_chunk, data_size, next);
					};
				decoded = reader->read(write_functor, kMaximumDecodedChunk);
			}
			else
			{
				const rapidgzip::GzipReader::WriteFunctor write_functor =
					[this, &next](const void *data, std::uint64_t data_size) {
						append_serial_chunk(data, data_size, next);
					};
				decoded = serial_reader->read(write_functor, kMaximumDecodedChunk);
			}
			if (cancelled.load())
			{
				finished = true;
				return 0;
			}
			if (decoded == 0)
			{
				finished = true;
				return 0;
			}
			if (next.spans.empty() || span_bytes(next) != decoded)
			{
				throw std::runtime_error("rapidgzip decoded chunk size mismatch");
			}
			chunk = std::move(next);
			return 1;
		}
		catch (const Cancelled &)
		{
			finished = true;
			return 0;
		}
		catch (const std::exception &exception)
		{
			if (cancelled.load())
			{
				finished = true;
				return 0;
			}
			error = exception_message(exception, path);
			message = error;
			finished = true;
			return -1;
		}
		catch (...)
		{
			if (cancelled.load())
			{
				finished = true;
				return 0;
			}
			error = "unknown rapidgzip decoder exception for " + path;
			message = error;
			finished = true;
			return -1;
		}
	}

	bool consume(std::size_t size, std::string &message)
	{
		std::size_t remaining = size;
		while (remaining > 0)
		{
			if (!has_data())
			{
				message = "rapidgzip consume exceeded the available decoded data";
				return false;
			}
			const Span &span = chunk.spans[chunk.span_index];
			const std::size_t available = span.size - chunk.span_offset;
			const std::size_t take = std::min(remaining, available);
			chunk.span_offset += take;
			remaining -= take;
			decoded_bytes += take;
			if (chunk.span_offset == span.size)
			{
				++chunk.span_index;
				chunk.span_offset = 0;
			}
		}
		if (!has_data())
		{
			chunk = DecodedChunk();
		}
		return true;
	}

	void cancel()
	{
		cancelled.store(true);
	}
};

extern "C" SoapnukeRapidGzip *soapnuke_rapidgzip_open(const char *path,
	std::size_t decoder_threads, char *error, std::size_t error_size)
{
	if (path == NULL || path[0] == '\0')
	{
		write_error(error, error_size, "rapidgzip input path is empty");
		return NULL;
	}
	if (decoder_threads == 0)
	{
		write_error(error, error_size, "rapidgzip decoder thread count must be greater than zero");
		return NULL;
	}
	std::unique_ptr<SoapnukeRapidGzip> result(new SoapnukeRapidGzip());
	result->path = path;
	result->decoder_threads = decoder_threads;
	try
	{
		std::unique_ptr<rapidgzip::StandardFileReader> file_reader(
			new rapidgzip::StandardFileReader(result->path));
		if (decoder_threads == 1)
		{
			result->serial_reader.reset(new rapidgzip::GzipReader(std::move(file_reader)));
			result->serial_reader->setCRC32Enabled(true);
		}
		else
		{
			result->reader.reset(new rapidgzip::ParallelGzipReader<>(
				std::move(file_reader), decoder_threads, kReaderBlockSize));
			result->reader->setCRC32Enabled(true);
			result->reader->setKeepIndex(false);
			result->reader->setMaxDecompressedChunkSize(kMaximumDecodedChunk);
		}
		return result.release();
	}
	catch (const std::exception &exception)
	{
		write_error(error, error_size, exception_message(exception, result->path));
		return NULL;
	}
}

extern "C" int soapnuke_rapidgzip_available(SoapnukeRapidGzip *reader,
	const unsigned char **data, std::size_t *size, char *error, std::size_t error_size)
{
	if (reader == NULL)
	{
		write_error(error, error_size, "rapidgzip reader is null");
		return -1;
	}
	std::string message;
	const int result = reader->available(data, size, message);
	if (result < 0)
	{
		write_error(error, error_size, message);
	}
	return result;
}

extern "C" int soapnuke_rapidgzip_refill(SoapnukeRapidGzip *reader,
	char *error, std::size_t error_size)
{
	if (reader == NULL)
	{
		write_error(error, error_size, "rapidgzip reader is null");
		return -1;
	}
	std::string message;
	const int result = reader->refill(message);
	if (result < 0)
	{
		write_error(error, error_size, message);
	}
	return result;
}

extern "C" int soapnuke_rapidgzip_consume(SoapnukeRapidGzip *reader,
	std::size_t size, char *error, std::size_t error_size)
{
	if (reader == NULL)
	{
		write_error(error, error_size, "rapidgzip reader is null");
		return -1;
	}
	std::string message;
	if (!reader->consume(size, message))
	{
		write_error(error, error_size, message);
		return -1;
	}
	return 0;
}

extern "C" void soapnuke_rapidgzip_cancel(SoapnukeRapidGzip *reader)
{
	if (reader != NULL)
	{
		reader->cancel();
	}
}

extern "C" void soapnuke_rapidgzip_close(SoapnukeRapidGzip *reader)
{
	if (reader == NULL)
	{
		return;
	}
	reader->cancel();
	reader->reader.reset();
	reader->serial_reader.reset();
	delete reader;
}

extern "C" std::size_t soapnuke_rapidgzip_decoder_threads(const SoapnukeRapidGzip *reader)
{
	return reader == NULL ? 0 : reader->decoder_threads;
}

extern "C" std::size_t soapnuke_rapidgzip_decoded_bytes(const SoapnukeRapidGzip *reader)
{
	return reader == NULL ? 0 : reader->decoded_bytes;
}

extern "C" int soapnuke_rapidgzip_parallel_reader_active(const SoapnukeRapidGzip *reader)
{
    return reader != NULL && reader->reader ? 1 : 0;
}
