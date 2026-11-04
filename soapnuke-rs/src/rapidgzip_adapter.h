#ifndef SOAPNUKE_RAPIDGZIP_ADAPTER_H
#define SOAPNUKE_RAPIDGZIP_ADAPTER_H

#include <stddef.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct SoapnukeRapidGzip SoapnukeRapidGzip;

SoapnukeRapidGzip *soapnuke_rapidgzip_open(const char *path,

    size_t decoder_threads,

    char *error,

    size_t error_size);

/* Return 1 when a contiguous decoded span is available, 2 when the owning
 * lane must call refill, 0 at clean end-of-stream, and -1 after a decoder
 * error. Refill performs the next bounded decode synchronously on the owning
 * lane thread; 2 does not indicate a background producer. */
int soapnuke_rapidgzip_available(SoapnukeRapidGzip *reader,

    const unsigned char **data,

    size_t *size,

    char *error,

    size_t error_size);

/* Decode at most one bounded chunk on the owning lane thread. */
int soapnuke_rapidgzip_refill(SoapnukeRapidGzip *reader,

    char *error,

    size_t error_size);

int soapnuke_rapidgzip_consume(SoapnukeRapidGzip *reader,

    size_t size,

    char *error,

    size_t error_size);

void soapnuke_rapidgzip_cancel(SoapnukeRapidGzip *reader);
void soapnuke_rapidgzip_close(SoapnukeRapidGzip *reader);
int soapnuke_rapidgzip_parallel_reader_active(const SoapnukeRapidGzip *reader);
size_t soapnuke_rapidgzip_decoder_threads(const SoapnukeRapidGzip *reader);
size_t soapnuke_rapidgzip_decoded_bytes(const SoapnukeRapidGzip *reader);

#ifdef __cplusplus
}
#endif

#endif
