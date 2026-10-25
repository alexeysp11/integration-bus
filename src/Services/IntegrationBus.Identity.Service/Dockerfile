# This stage is used when running from VS in fast mode (Default for Debug configuration)
FROM mcr.microsoft.com/dotnet/aspnet:10.0 AS base
USER $APP_UID
WORKDIR /app


# This stage is used to build the service project
FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["src/Services/IntegrationBus.Identity.Service/IntegrationBus.Identity.Service.csproj", "src/Services/IntegrationBus.Identity.Service/"]
COPY ["src/Contracts/IntegrationBus.Identity.Contracts/IntegrationBus.Identity.Contracts.csproj", "src/Contracts/IntegrationBus.Identity.Contracts/"]
COPY ["src/Contracts/IntegrationBus.Contracts/IntegrationBus.Contracts.csproj", "src/Contracts/IntegrationBus.Contracts/"]
COPY ["src/Libraries/IntegrationBus.Shared/IntegrationBus.Shared.csproj", "src/Libraries/IntegrationBus.Shared/"]
RUN dotnet restore "./src/Services/IntegrationBus.Identity.Service/IntegrationBus.Identity.Service.csproj"
COPY . .
WORKDIR "/src/src/Services/IntegrationBus.Identity.Service"
RUN dotnet build "./IntegrationBus.Identity.Service.csproj" -c $BUILD_CONFIGURATION -o /app/build

# This stage is used to publish the service project to be copied to the final stage
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./IntegrationBus.Identity.Service.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

# This stage is used in production or when running from VS in regular mode (Default when not using the Debug configuration)
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "IntegrationBus.Identity.Service.dll"]
