/// SIMD-accelerated columnar encoding operations
/// Supports AVX2 (x86_64) and NEON (ARM64) with scalar fallback

/// Sum an array of i64 values using SIMD
#[cfg(target_arch = "x86_64")]
pub fn sum_i64_simd(values: &[i64]) -> i64 {
    if values.len() < 4 {
        return values.iter().sum();
    }

    #[cfg(target_feature = "avx2")]
    unsafe {
        sum_i64_avx2(values)
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        values.iter().sum()
    }
}

#[cfg(target_arch = "x86_64")]
#[cfg(target_feature = "avx2")]
unsafe fn sum_i64_avx2(values: &[i64]) -> i64 {
    use std::arch::x86_64::*;
    let mut sum = _mm256_setzero_si256();
    let chunks = values.chunks_exact(4);
    let remainder = chunks.remainder();

    for chunk in chunks {
        let v = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
        sum = _mm256_add_epi64(sum, v);
    }

    // Horizontal sum
    let hi = _mm256_extracti128_si256(sum, 1);
    let lo = _mm256_castsi256_si128(sum);
    let sum128 = _mm_add_epi64(hi, lo);
    let hi64 = _mm_extract_epi64(sum128, 1);
    let lo64 = _mm_extract_epi64(sum128, 0);
    let mut result = hi64 + lo64;

    for &v in remainder {
        result += v;
    }
    result
}

/// Sum an array of i64 values (scalar fallback)
#[cfg(not(target_arch = "x86_64"))]
pub fn sum_i64_simd(values: &[i64]) -> i64 {
    values.iter().sum()
}

/// Find min/max of i64 array using SIMD
#[cfg(target_arch = "x86_64")]
pub fn min_max_i64_simd(values: &[i64]) -> (i64, i64) {
    if values.is_empty() {
        return (0, 0);
    }
    if values.len() < 4 {
        let min = *values.iter().min().unwrap();
        let max = *values.iter().max().unwrap();
        return (min, max);
    }

    #[cfg(target_feature = "avx2")]
    unsafe {
        min_max_i64_avx2(values)
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        let min = *values.iter().min().unwrap();
        let max = *values.iter().max().unwrap();
        (min, max)
    }
}

#[cfg(target_arch = "x86_64")]
#[cfg(target_feature = "avx2")]
unsafe fn min_max_i64_avx2(values: &[i64]) -> (i64, i64) {
    use std::arch::x86_64::*;
    let mut min_vec = _mm256_set1_epi64x(i64::MAX);
    let mut max_vec = _mm256_set1_epi64x(i64::MIN);

    for chunk in values.chunks_exact(4) {
        let v = _mm256_loadu_si256(chunk.as_ptr() as *const __m256i);
        min_vec = _mm256_min_epi64(min_vec, v);
        max_vec = _mm256_max_epi64(max_vec, v);
    }

    // Extract results
    let min_lo = _mm256_castsi256_si128(min_vec);
    let min_hi = _mm256_extracti128_si256(min_vec, 1);
    let min128 = _mm_min_epi64(min_lo, min_hi);
    let min = _mm_extract_epi64(min128, 0).min(_mm_extract_epi64(min128, 1));

    let max_lo = _mm256_castsi256_si128(max_vec);
    let max_hi = _mm256_extracti128_si256(max_vec, 1);
    let max128 = _mm_max_epi64(max_lo, max_hi);
    let max = _mm_extract_epi64(max128, 0).max(_mm_extract_epi64(max128, 1));

    // Handle remainder
    for &v in values.chunks_exact(4).remainder() {
        let min = min.min(v);
        let max = max.max(v);
        return (min, max);
    }

    (min, max)
}

/// Find min/max of i64 array (scalar fallback)
#[cfg(not(target_arch = "x86_64"))]
pub fn min_max_i64_simd(values: &[i64]) -> (i64, i64) {
    if values.is_empty() {
        return (0, 0);
    }
    let min = *values.iter().min().unwrap();
    let max = *values.iter().max().unwrap();
    (min, max)
}

/// XOR encode array of f64 values using SIMD
#[cfg(target_arch = "x86_64")]
pub fn xor_encode_f64_simd(values: &[f64]) -> Vec<u64> {
    if values.len() < 4 {
        return values.iter().map(|v| v.to_bits()).collect();
    }

    #[cfg(target_feature = "avx2")]
    unsafe {
        xor_encode_f64_avx2(values)
    }
    #[cfg(not(target_feature = "avx2"))]
    {
        values.iter().map(|v| v.to_bits()).collect()
    }
}

#[cfg(target_arch = "x86_64")]
#[cfg(target_feature = "avx2")]
unsafe fn xor_encode_f64_avx2(values: &[f64]) -> Vec<u64> {
    use std::arch::x86_64::*;
    let mut result = Vec::with_capacity(values.len());
    let mut prev = _mm256_setzero_pd();

    for chunk in values.chunks_exact(4) {
        let v = _mm256_loadu_pd(chunk.as_ptr());
        let xor = _mm256_xor_pd(v, prev);
        let bits = std::mem::transmute::<__m256d, [u64; 4]>(xor);
        result.extend_from_slice(&bits);
        prev = v;
    }

    // Handle remainder
    for &v in values.chunks_exact(4).remainder() {
        let bits = v.to_bits();
        result.push(bits);
    }

    result
}

/// XOR encode array of f64 values (scalar fallback)
#[cfg(not(target_arch = "x86_64"))]
pub fn xor_encode_f64_simd(values: &[f64]) -> Vec<u64> {
    values.iter().map(|v| v.to_bits()).collect()
}

/// Count leading zeros of u64 using SIMD
pub fn leading_zeros_simd(values: &[u64]) -> Vec<u32> {
    values.iter().map(|v| v.leading_zeros()).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sum_i64_simd() {
        let values: Vec<i64> = (0..1000).collect();
        assert_eq!(sum_i64_simd(&values), 499500);
    }

    #[test]
    fn test_min_max_i64_simd() {
        let values = vec![5, 3, 8, 1, 9, 2, 7, 4, 6, 0];
        let (min, max) = min_max_i64_simd(&values);
        assert_eq!(min, 0);
        assert_eq!(max, 9);
    }

    #[test]
    fn test_xor_encode_f64_simd() {
        let values = vec![1.0, 2.0, 3.0, 4.0];
        let result = xor_encode_f64_simd(&values);
        assert_eq!(result.len(), 4);
        // First value should be raw bits
        assert_eq!(result[0], 1.0_f64.to_bits());
        // XOR of consecutive values should be non-zero
        assert!(result[1] != 0 || result[2] != 0);
    }

    #[test]
    fn test_leading_zeros_simd() {
        let values = vec![0u64, 1, 2, 4, 8];
        let result = leading_zeros_simd(&values);
        assert_eq!(result[0], 64);
        assert_eq!(result[1], 63);
        assert_eq!(result[2], 62);
        assert_eq!(result[3], 61);
        assert_eq!(result[4], 60);
    }
}
