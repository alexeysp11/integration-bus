using Microsoft.AspNetCore.Mvc.Filters;
using Microsoft.AspNetCore.Mvc;

namespace IntegrationBus.Processing.Api.Filters;

/// <summary>
/// Blocks endpoint execution and forces an HTTP 404 response when running under Production environment profile.
/// </summary>
[AttributeUsage(AttributeTargets.Method | AttributeTargets.Class)]
public sealed class DenyProductionEnvironmentAttribute : ActionFilterAttribute
{
    /// <summary>
    /// Evaluates the active hosting environment profile before executing the target action pipeline.
    /// </summary>
    public override void OnActionExecuting(ActionExecutingContext context)
    {
        IWebHostEnvironment environment = context.HttpContext.RequestServices.GetRequiredService<IWebHostEnvironment>();

        if (environment.IsProduction())
        {
            context.Result = new NotFoundResult();
        }
    }
}
