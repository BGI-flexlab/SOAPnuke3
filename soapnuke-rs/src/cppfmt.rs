// C++ iostream-compatible formatting helpers.
// Verified against libstdc++: integer types ignore precision & floatfield;
// floats honor fixed + setprecision(N).
#[inline]
pub fn fmt_u(v: u64) -> String {
    v.to_string()
}

#[inline]
pub fn fmt_fixed(v: f32, prec: usize) -> String {
    format!("{:.prec$}", v, prec = prec)
}

#[inline]
pub fn fmt_fixed_f64(v: f64, prec: usize) -> String {
    format!("{:.prec$}", v, prec = prec)
}

// sprintf("%.2f", double) equivalent used for filter ratios.
#[inline]
pub fn sprintf_2f(v: f32) -> String {
    format!("{:.2}", v as f64)
}
