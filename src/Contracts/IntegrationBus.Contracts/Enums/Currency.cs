namespace IntegrationBus.Contracts.Enums;

/// <summary>
/// Specifies the supported global, regional, and specific national financial currency variants within the ecosystem.
/// </summary>
public enum Currency
{
    /// <summary>
    /// Unspecified or invalid currency placeholder used to catch uninitialized data.
    /// </summary>
    None = 0,

    /// <summary>
    /// United States Dollar.
    /// </summary>
    USD = 1,

    /// <summary>
    /// Euro.
    /// </summary>
    EUR = 2,

    /// <summary>
    /// Swiss Franc.
    /// </summary>
    CHF = 3,

    /// <summary>
    /// Australian Dollar.
    /// </summary>
    AUD = 4,

    /// <summary>
    /// Canadian Dollar.
    /// </summary>
    CAD = 5,

    /// <summary>
    /// United Arab Emirates Dirham.
    /// </summary>
    AED = 6,

    /// <summary>
    /// Georgian Lari.
    /// </summary>
    GEL = 7,

    /// <summary>
    /// Japanese Yen.
    /// </summary>
    JPY = 8,

    /// <summary>
    /// Chinese Yuan / Renminbi.
    /// </summary>
    CNY = 9,

    /// <summary>
    /// Russian Ruble.
    /// </summary>
    RUB = 10,

    /// <summary>
    /// Belarusian Ruble.
    /// </summary>
    BYN = 11
}
