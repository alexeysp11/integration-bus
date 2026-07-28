using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Mvc.Abstractions;
using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Routing;
using Microsoft.Extensions.Hosting;
using Moq;
using FluentAssertions;
using IntegrationBus.Processing.Api.Filters;
using Microsoft.AspNetCore.Hosting;

namespace IntegrationBus.Processing.Api.Tests.Filters;

public sealed class DenyProductionEnvironmentAttributeTests
{
    [Fact(DisplayName = "DenyProductionEnvironment filter forces HTTP 404 NotFound result when the active environment profile is Production")]
    public void OnActionExecuting_ShouldSetNotFoundResult_WhenEnvironmentIsProduction()
    {
        // Arrange
        DenyProductionEnvironmentAttribute filter = new();

        Mock<IWebHostEnvironment> mockEnv = new();
        mockEnv.Setup(e => e.EnvironmentName).Returns(Environments.Production);

        Mock<IServiceProvider> mockServiceProvider = new();
        mockServiceProvider.Setup(s => s.GetService(typeof(IWebHostEnvironment))).Returns(mockEnv.Object);

        DefaultHttpContext httpContext = new()
        {
            RequestServices = mockServiceProvider.Object
        };

        ActionExecutingContext context = new(
            new ActionContext(httpContext, new RouteData(), new ActionDescriptor()),
            [],
            new Dictionary<string, object?>(),
            new Mock<Controller>().Object
        );

        // Act
        filter.OnActionExecuting(context);

        // Assert
        context.Result.Should().NotBeNull().And.BeOfType<NotFoundResult>();
    }

    [Fact(DisplayName = "DenyProductionEnvironment filter passes execution seamlessly when the active environment profile is Development")]
    public void OnActionExecuting_ShouldNotModifyResult_WhenEnvironmentIsDevelopment()
    {
        // Arrange
        DenyProductionEnvironmentAttribute filter = new();

        Mock<IWebHostEnvironment> mockEnv = new();
        mockEnv.Setup(e => e.EnvironmentName).Returns(Environments.Development);

        Mock<IServiceProvider> mockServiceProvider = new();
        mockServiceProvider.Setup(s => s.GetService(typeof(IWebHostEnvironment))).Returns(mockEnv.Object);

        DefaultHttpContext httpContext = new()
        {
            RequestServices = mockServiceProvider.Object
        };

        ActionExecutingContext context = new(
            new ActionContext(httpContext, new RouteData(), new ActionDescriptor()),
            [],
            new Dictionary<string, object?>(),
            new Mock<Controller>().Object
        );

        // Act
        filter.OnActionExecuting(context);

        // Assert
        context.Result.Should().BeNull();
    }
}
