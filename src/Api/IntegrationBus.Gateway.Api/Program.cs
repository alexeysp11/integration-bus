WebApplicationBuilder builder = WebApplication.CreateBuilder(args);

builder.Services.AddOpenApi();

builder.Services.AddReverseProxy()
    .LoadFromConfig(builder.Configuration.GetSection("ReverseProxy"));

WebApplication app = builder.Build();

if (app.Environment.IsDevelopment())
{
    app.MapOpenApi();
}

app.UseHttpsRedirection();
app.MapReverseProxy();

app.Run();
