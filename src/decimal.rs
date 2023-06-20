//! Module defining the `Decimal` type.
//!
//! This is a stripped-down version of this type from liquidity-pools project.
//! To be extracted as a separate lib.

use bigdecimal::BigDecimal;
use std::fmt;

/// Decimal value which can be converted to `f64` or `BigDecimal`.
///
/// Internally stored as an integer value and scale,
/// where the latter can be either unknown (and needs to be specified elsewhere),
/// or expressed as either a number of decimal points or a power-of-ten divisor.
///
/// ```
/// # use bigdecimal::BigDecimal;
/// # use contracts::Decimal;
/// // Unknown scale, raw value is `12345`
/// let x = Decimal::new_raw(12345);
/// assert_eq!(x.raw_value(), 12345);
/// assert_eq!(x.with_scale(1).to_f64(), 1234.5); // Apply some scale
/// assert_eq!(x.with_divisor(10).to_f64(), 1234.5); // Apply some divisor
///
/// // Scaled by 2 decimal points: `123.45`
/// let x = Decimal::new_fixed(12345, 2);
/// assert_eq!(x.to_f64(), 123.45);
///
/// // Scaled by 1/1000: `12.345`
/// let x = Decimal::new_divisor(12345, 1000);
/// assert_eq!(x.to_f64(), 12.345);
///
/// // If the scale is known, the value can be converted to `BigDecimal` (or `f64`):
/// let x: BigDecimal = Decimal::new_fixed(42, 2).to_big();
/// assert_eq!(x, BigDecimal::from(0.42));
/// ```
#[derive(Clone)]
pub struct Decimal<S: Scale> {
    raw_value: i64,
    scale: S,
}

/// A marker trait that defines a scale which can be applied to a `Decimal` value.
///
/// This trait is unlikely to be used directly, instead use one of its implementations:
/// `UnknownScale`, `Fixed` or `Divisor`.
pub trait Scale {}

/// A type indicating that a particular `Decimal` value has some yet unknown decimal points,
/// so only raw (unscaled) value can be used until the decimal points is obtained elsewhere.
#[derive(Clone, Copy, PartialEq, Eq)]
pub struct UnknownScale;

impl Scale for UnknownScale {}

/// A type indicating that a fixed, known scale (number of decimal points) to be applied to
/// a `Decimal` to get a real value.
///
/// ```
/// # use contracts::{Decimal, Fixed};
/// let x: Decimal<Fixed> = Decimal::new_fixed(123, 2);
/// assert_eq!(x.to_f64(), 1.23);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Fixed(pub u8);

impl Scale for Fixed {}

/// A type indicating that a fixed divisor to be applied to a `Decimal` to get a real value.
/// The divisor must be a power of ten, i.e. 1, 10, 100, 1000 etc.
///
/// ```
/// # use contracts::{Decimal, Divisor};
/// let x: Decimal<Divisor> = Decimal::new_divisor(123, 100);
/// assert_eq!(x.to_f64(), 1.23);
/// ```
#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct Divisor(pub u64);

impl Scale for Divisor {}

impl<S: Scale> Decimal<S> {
    /// Raw (unscaled) integer value of this `Decimal`.
    ///
    /// ```
    /// # use contracts::Decimal;
    /// let x = Decimal::new_fixed(123, 2);
    /// assert_eq!(x.raw_value(), 123);
    /// ```
    #[inline]
    pub fn raw_value(&self) -> i64 {
        self.raw_value
    }
}

impl Decimal<UnknownScale> {
    /// Create `Decimal` value with an unknown decimal points.
    ///
    /// ```
    /// # use contracts::{Decimal, UnknownScale};
    /// let x: Decimal<UnknownScale> = Decimal::new_raw(123);
    /// assert_eq!(x.raw_value(), 123);
    /// ```
    #[inline]
    pub fn new_raw(raw_value: i64) -> Self {
        Decimal {
            raw_value,
            scale: UnknownScale,
        }
    }

    /// Convert an unknown-precision value into a known by adding information about decimal points.
    ///
    /// ```
    /// # use contracts::{Decimal, UnknownScale};
    /// let x: Decimal<UnknownScale> = Decimal::new_raw(123);
    /// assert_eq!(x.with_scale(2).to_f64(), 1.23);
    /// ```
    #[inline]
    pub fn with_scale(&self, scale: u8) -> Decimal<Fixed> {
        Decimal::new_fixed(self.raw_value, scale)
    }

    /// Convert an unknown-precision value into a known by adding information about decimal points.
    ///
    /// ```
    /// # use contracts::{Decimal, UnknownScale};
    /// let x: Decimal<UnknownScale> = Decimal::new_raw(123);
    /// assert_eq!(x.with_divisor(100).to_f64(), 1.23);
    /// ```
    #[inline]
    pub fn with_divisor(&self, divisor: u64) -> Decimal<Divisor> {
        Decimal::new_divisor(self.raw_value, divisor)
    }
}

impl Decimal<Fixed> {
    /// Create `Decimal` value with known fixed number of decimal points.
    ///
    /// ```
    /// # use contracts::Decimal;
    /// let x = Decimal::new_fixed(123, 2);
    /// assert_eq!(x.to_f64(), 1.23);
    /// ```
    #[inline]
    pub fn new_fixed(raw_value: i64, decimals: u8) -> Self {
        assert!(
            decimals <= 30,
            "Suspicious number of decimals: {}",
            decimals
        );
        Decimal {
            raw_value,
            scale: Fixed(decimals),
        }
    }

    /// Number of decimal points in this value.
    ///
    /// ```
    /// # use contracts::Decimal;
    /// let x = Decimal::new_fixed(123, 2);
    /// assert_eq!(x.decimals(), 2);
    /// ```
    #[inline]
    pub fn decimals(&self) -> u8 {
        let Fixed(scale) = self.scale;
        scale
    }

    /// Convert to `f64` by applying scale to the raw value.
    ///
    /// ```
    /// # use contracts::Decimal;
    /// let x = Decimal::new_fixed(123, 2);
    /// assert_eq!(x.to_f64(), 1.23);
    /// ```
    #[inline]
    pub fn to_f64(&self) -> f64 {
        (self.raw_value() as f64) / 10_f64.powf(self.decimals() as f64)
    }

    /// Convert to `BigDecimal` by applying scale to the raw value.
    ///
    /// ```
    /// # use bigdecimal::BigDecimal;
    /// # use contracts::Decimal;
    /// let x = Decimal::new_fixed(123, 2);
    /// assert_eq!(x.to_big(), BigDecimal::from(1.23));
    /// ```
    #[inline]
    pub fn to_big(&self) -> BigDecimal {
        let x = BigDecimal::from(self.raw_value());
        let y = BigDecimal::from(10_i64.pow(self.decimals() as u32));
        x / y
    }
}

impl Decimal<Divisor> {
    /// Create `Decimal` value with known precision expressed as an integer divisor.
    ///
    /// ```
    /// # use contracts::Decimal;
    /// let x = Decimal::new_divisor(123, 100);
    /// assert_eq!(x.to_f64(), 1.23);
    /// ```
    #[inline]
    pub fn new_divisor(raw_value: i64, divisor: u64) -> Self {
        let is_power_of_ten = {
            let log10 = f64::log10(divisor as f64);
            log10 - log10.floor() == 0.0
        };
        assert!(
            divisor > 0 && is_power_of_ten,
            "Decimal divisor must be a power of 10: {}",
            divisor
        );
        Decimal {
            raw_value,
            scale: Divisor(divisor),
        }
    }

    /// Divisor of this value.
    ///
    /// ```
    /// # use contracts::Decimal;
    /// let x = Decimal::new_divisor(123, 100);
    /// assert_eq!(x.divisor(), 100);
    /// ```
    #[inline]
    pub fn divisor(&self) -> u64 {
        let Divisor(divisor) = self.scale;
        divisor
    }

    /// Convert to `f64` by applying scale to the raw value.
    ///
    /// ```
    /// # use contracts::Decimal;
    /// let x = Decimal::new_divisor(123, 100);
    /// assert_eq!(x.to_f64(), 1.23);
    /// ```
    #[inline]
    pub fn to_f64(&self) -> f64 {
        (self.raw_value() as f64) / (self.divisor() as f64)
    }

    /// Convert to `BigDecimal` by applying scale to the raw value.
    ///
    /// ```
    /// # use bigdecimal::BigDecimal;
    /// # use contracts::Decimal;
    /// let x = Decimal::new_divisor(123, 100);
    /// assert_eq!(x.to_big(), BigDecimal::from(1.23));
    /// ```
    #[inline]
    pub fn to_big(&self) -> BigDecimal {
        let x = BigDecimal::from(self.raw_value());
        let y = BigDecimal::from(self.divisor());
        x / y
    }
}

#[rustfmt::skip]
#[test]
fn test_decimal_fixed_to_f64() {
    assert_eq!(Decimal::new_fixed(12345, 0).to_f64(), 12345.0);
    assert_eq!(Decimal::new_fixed(12345, 1).to_f64(), 1234.5);
    assert_eq!(Decimal::new_fixed(12345, 2).to_f64(), 123.45);
    assert_eq!(Decimal::new_fixed(12345, 3).to_f64(), 12.345);
    assert_eq!(Decimal::new_fixed(12345, 4).to_f64(), 1.2345);
    assert_eq!(Decimal::new_fixed(12345, 5).to_f64(), 0.12345);
}

#[rustfmt::skip]
#[test]
fn test_decimal_fixed_to_bigdecimal() {
    assert_eq!(Decimal::new_fixed(12345, 0).to_big(), BigDecimal::new(12345.into(), 0));
    assert_eq!(Decimal::new_fixed(12345, 1).to_big(), BigDecimal::new(12345.into(), 1));
    assert_eq!(Decimal::new_fixed(12345, 2).to_big(), BigDecimal::new(12345.into(), 2));
    assert_eq!(Decimal::new_fixed(12345, 3).to_big(), BigDecimal::new(12345.into(), 3));
    assert_eq!(Decimal::new_fixed(12345, 4).to_big(), BigDecimal::new(12345.into(), 4));
    assert_eq!(Decimal::new_fixed(12345, 5).to_big(), BigDecimal::new(12345.into(), 5));
}

#[rustfmt::skip]
#[test]
fn test_decimal_divisor_to_f64() {
    assert_eq!(Decimal::new_divisor(12345, 1).to_f64(), 12345.0);
    assert_eq!(Decimal::new_divisor(12345, 10).to_f64(), 1234.5);
    assert_eq!(Decimal::new_divisor(12345, 100).to_f64(), 123.45);
    assert_eq!(Decimal::new_divisor(12345, 1000).to_f64(), 12.345);
    assert_eq!(Decimal::new_divisor(12345, 10000).to_f64(), 1.2345);
    assert_eq!(Decimal::new_divisor(12345, 100000).to_f64(), 0.12345);
}

#[rustfmt::skip]
#[test]
fn test_decimal_divisor_to_bigdecimal() {
    assert_eq!(Decimal::new_divisor(12345, 1).to_big(), BigDecimal::new(12345.into(), 0));
    assert_eq!(Decimal::new_divisor(12345, 10).to_big(), BigDecimal::new(12345.into(), 1));
    assert_eq!(Decimal::new_divisor(12345, 100).to_big(), BigDecimal::new(12345.into(), 2));
    assert_eq!(Decimal::new_divisor(12345, 1000).to_big(), BigDecimal::new(12345.into(), 3));
    assert_eq!(Decimal::new_divisor(12345, 10000).to_big(), BigDecimal::new(12345.into(), 4));
    assert_eq!(Decimal::new_divisor(12345, 100000).to_big(), BigDecimal::new(12345.into(), 5));
}

#[test]
#[should_panic]
fn test_decimal_bad_scale() {
    // This will panic because 200 is unlikely a valid number of decimal points
    let _ = Decimal::new_fixed(12345, 200);
}

#[test]
#[should_panic]
fn test_decimal_bad_divisor() {
    // This will panic because 42 is not a power of ten
    let _ = Decimal::new_divisor(12345, 42);
}

// From/Into

impl From<Decimal<Fixed>> for f64 {
    #[inline]
    fn from(value: Decimal<Fixed>) -> Self {
        value.to_f64()
    }
}

impl From<Decimal<Divisor>> for f64 {
    #[inline]
    fn from(value: Decimal<Divisor>) -> Self {
        value.to_f64()
    }
}

impl From<Decimal<Fixed>> for BigDecimal {
    #[inline]
    fn from(value: Decimal<Fixed>) -> Self {
        value.to_big()
    }
}

impl From<Decimal<Divisor>> for BigDecimal {
    #[inline]
    fn from(value: Decimal<Divisor>) -> Self {
        value.to_big()
    }
}

// Formatting

impl fmt::Debug for Decimal<UnknownScale> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}*10^?", self.raw_value())
    }
}

impl fmt::Debug for Decimal<Fixed> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}*10^-{}", self.raw_value(), self.decimals())
    }
}

impl fmt::Debug for Decimal<Divisor> {
    #[inline]
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.raw_value(), self.divisor())
    }
}
