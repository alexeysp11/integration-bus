using System.Reflection;
using IntegrationBus.Contracts.Security;
using MassTransit;

namespace IntegrationBus.Shared.Security;

/// <summary>
/// Infrastructure-level MassTransit consume filter: for every message carrying one or more
/// <see cref="SensitiveDataAttribute"/>-tagged properties, mirrors an HMAC-masked shadow copy onto
/// <c>{sourceTopic}.security</c> before handing the untouched original message to the real consumer.
/// </summary>
/// <typeparam name="T">The message contract type this filter instance is bound to.</typeparam>
public sealed class SensitiveDataMaskingFilter<T> : IFilter<ConsumeContext<T>> where T : class
{
    private static readonly PropertyInfo[] SensitiveProperties = typeof(T)
        .GetProperties()
        .Where(property => property.GetCustomAttribute<SensitiveDataAttribute>() is not null)
        .ToArray();

    private readonly string _sourceTopic;
    private readonly IHmacMaskingService _maskingService;
    private readonly ISecurityTopicPublisher _securityPublisher;

    public SensitiveDataMaskingFilter(
        string sourceTopic,
        IHmacMaskingService maskingService,
        ISecurityTopicPublisher securityPublisher)
    {
        _sourceTopic = sourceTopic;
        _maskingService = maskingService;
        _securityPublisher = securityPublisher;
    }

    /// <inheritdoc />
    public async Task Send(ConsumeContext<T> context, IPipe<ConsumeContext<T>> next)
    {
        if (SensitiveProperties.Length > 0)
        {
            Dictionary<string, string> maskedFields = SensitiveProperties.ToDictionary(
                property => property.Name,
                property => _maskingService.MaskValue(property.GetValue(context.Message)));

            string key = context.MessageId?.ToString() ?? Guid.NewGuid().ToString();

            await _securityPublisher.PublishMaskedAsync(_sourceTopic, key, maskedFields, context.CancellationToken);
        }

        await next.Send(context);
    }

    /// <inheritdoc />
    public void Probe(ProbeContext context) => context.CreateFilterScope("sensitiveDataMasking");
}
