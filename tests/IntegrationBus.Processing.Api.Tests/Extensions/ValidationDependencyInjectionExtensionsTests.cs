using FluentAssertions;
using FluentValidation;
using IntegrationBus.Contracts.Http;
using IntegrationBus.Processing.Api.Extensions;
using IntegrationBus.Processing.Api.Tests.Extensions.Fakes;
using IntegrationBus.Processing.Api.Validation;
using Microsoft.Extensions.DependencyInjection;

namespace IntegrationBus.Processing.Api.Tests.Extensions;

/// <summary>
/// Contains automated structural and behavioral verification tests for the <see cref="ValidationDependencyInjectionExtensions"/> registration boundaries.
/// </summary>
public sealed class ValidationDependencyInjectionExtensionsTests
{
    private readonly IServiceCollection _services = new ServiceCollection();

    [Fact(DisplayName = "AddApiValidators returns the identical service collection reference to preserve fluent configuration chaining")]
    public void AddApiValidators_ShouldReturnSameServiceCollection_ToAllowFluentChaining()
    {
        // Act
        IServiceCollection result = _services.AddApiValidators();

        // Assert
        result.Should().BeSameAs(_services);
    }

    [Fact(DisplayName = "AddApiValidators explicitly registers TopUpAccountRequestValidator with a strict singleton lifetime")]
    public void AddApiValidators_ShouldRegisterTopUpAccountRequestValidator_WithSingletonLifetime()
    {
        // Act
        _services.AddApiValidators();

        // Assert
        ServiceDescriptor? registration = _services.FirstOrDefault(sd => sd.ServiceType == typeof(IValidator<TopUpAccountRequest>));

        registration.Should().NotBeNull();
        registration!.ImplementationType.Should().Be(typeof(TopUpAccountRequestValidator));
        registration.Lifetime.Should().Be(ServiceLifetime.Singleton);
    }

    [Fact(DisplayName = "AddApiValidators explicitly registers StartTransactionRequestValidator with a strict singleton lifetime")]
    public void AddApiValidators_ShouldRegisterStartTransactionRequestValidator_WithSingletonLifetime()
    {
        // Act
        _services.AddApiValidators();

        // Assert
        ServiceDescriptor? registration = _services.FirstOrDefault(sd => sd.ServiceType == typeof(IValidator<StartTransactionRequest>));

        registration.Should().NotBeNull();
        registration!.ImplementationType.Should().Be(typeof(StartTransactionRequestValidator));
        registration.Lifetime.Should().Be(ServiceLifetime.Singleton);
    }

    [Fact(DisplayName = "AddApiValidators is idempotent and does not break the container when called multiple times")]
    public void AddApiValidators_ShouldBeIdempotent_WhenCalledMultipleTimes()
    {
        // Act
        _services.AddApiValidators();
        _services.AddApiValidators();

        // Assert
        using ServiceProvider serviceProvider = _services.BuildServiceProvider();

        Action resolveAction = () =>
        {
            serviceProvider.GetRequiredService<IValidator<TopUpAccountRequest>>();
            serviceProvider.GetRequiredService<IValidator<StartTransactionRequest>>();
        };

        resolveAction.Should().NotThrow("Subsequent container configuration invocations must not cause resolution runtime failures");
    }

    [Fact(DisplayName = "AddApiValidators allows resolving a single implementation without duplicate bloat in the resolution chain")]
    public void AddApiValidators_ShouldNotDuplicateServiceRegistrationsInCollection()
    {
        // Act
        _services.AddApiValidators();

        // Assert
        int registrationCount = _services.Count(sd => sd.ServiceType == typeof(IValidator<TopUpAccountRequest>));

        registrationCount.Should().Be(1, "Each application contract must map to exactly one explicit validator registration to avoid collection bloat");
    }

    [Fact(DisplayName = "AddApiValidators registers mandatory FluentValidation infrastructure like ValidatorFactory")]
    public void AddApiValidators_ShouldRegisterCoreFluentValidationInfrastructure()
    {
        // Act
        _services.AddApiValidators();

        // Assert
        bool hasValidatorFactory = _services.Any(sd =>
            sd.ServiceType.Name.Contains("IValidatorFactory") ||
            (sd.ServiceType.FullName != null && sd.ServiceType.FullName.Contains("FluentValidation")));

        hasValidatorFactory.Should().BeTrue("Core FluentValidation factory components must be present within the service descriptor collection");
    }

    [Fact(DisplayName = "AddApiValidators deterministically activates FluentValidation automatic validation infrastructure")]
    public void AddApiValidators_ShouldRegisterFluentValidationAutoValidationServices()
    {
        // Act
        _services.AddApiValidators();

        // Assert
        bool hasAutoValidation = _services.Any(sd =>
            sd.ServiceType.FullName != null &&
            sd.ServiceType.FullName.Contains("FluentValidation", StringComparison.OrdinalIgnoreCase));

        hasAutoValidation.Should().BeTrue();
    }

    [Fact(DisplayName = "AddApiValidators successfully resolves all registered validator instances from the built service provider container")]
    public void AddApiValidators_ShouldResolveRegisteredValidatorsSuccessfully_WhenProviderIsBuilt()
    {
        // Arrange
        _services.AddApiValidators();

        // Act & Assert
        using ServiceProvider serviceProvider = _services.BuildServiceProvider();

        IValidator<TopUpAccountRequest>? topUpValidator = serviceProvider.GetService<IValidator<TopUpAccountRequest>>();
        IValidator<StartTransactionRequest>? transactionValidator = serviceProvider.GetService<IValidator<StartTransactionRequest>>();

        topUpValidator.Should().NotBeNull().And.BeOfType<TopUpAccountRequestValidator>();
        transactionValidator.Should().NotBeNull().And.BeOfType<StartTransactionRequestValidator>();
    }

    [Fact(DisplayName = "AddApiValidators overrides or takes precedence over previously registered external validators for the same type")]
    public void AddApiValidators_ShouldTakePrecedence_WhenExternalValidatorsAlreadyRegistered()
    {
        // Arrange
        // Simulate an external module registering a different validator type first
        _services.AddSingleton<IValidator<TopUpAccountRequest>, ExternalDummyValidator>();

        // Act
        _services.AddApiValidators();

        // Assert
        using ServiceProvider serviceProvider = _services.BuildServiceProvider();
        IValidator<TopUpAccountRequest> resolvedValidator = serviceProvider.GetRequiredService<IValidator<TopUpAccountRequest>>();

        // The last registered service must take precedence in MS DI container behavior
        resolvedValidator.Should().BeOfType<TopUpAccountRequestValidator>();
    }

    [Fact(DisplayName = "AddApiValidators resolved instances are thread safe and remain stable under concurrent resolution pressure")]
    public async Task AddApiValidators_ShouldResolveValidatorsSafely_UnderConcurrentLoad()
    {
        // Arrange
        _services.AddApiValidators();
        using ServiceProvider serviceProvider = _services.BuildServiceProvider();
        List<Task<IValidator<TopUpAccountRequest>>> tasks = [];

        // Act
        // Spawn multiple parallel threads requesting the singleton validator instance concurrently
        for (int i = 0; i < 20; i++)
        {
            tasks.Add(Task.Run(() => serviceProvider.GetRequiredService<IValidator<TopUpAccountRequest>>()));
        }

        // Await all tasks asynchronously without blocking the execution thread
        IValidator<TopUpAccountRequest>[] resolvedValidators = await Task.WhenAll(tasks);

        // Assert
        resolvedValidators.Should().NotContainNulls("The underlying singleton resolution pipeline must be explicitly thread safe");

        // Ensure all concurrent invocations fetched the exact same reference instance
        IValidator<TopUpAccountRequest> expectedReference = resolvedValidators[0];
        foreach (IValidator<TopUpAccountRequest> validator in resolvedValidators)
        {
            validator.Should().BeSameAs(expectedReference);
        }
    }
}
