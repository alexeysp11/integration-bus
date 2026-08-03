# See https://aka.ms/customizecontainer to learn how to customize your debug container and how Visual Studio uses this Dockerfile to build your images for faster debugging.

# This stage is used when running from VS in fast mode (Default for Debug configuration)
FROM mcr.microsoft.com/dotnet/runtime:9.0 AS base
USER $APP_UID
WORKDIR /app


# This stage is used to build the service project
FROM mcr.microsoft.com/dotnet/sdk:9.0 AS build
ARG BUILD_CONFIGURATION=Release
WORKDIR /src
COPY ["src/Services/IntegrationBus.SagaOrchestrator.Service/IntegrationBus.SagaOrchestrator.Service.csproj", "src/Services/IntegrationBus.SagaOrchestrator.Service/"]
COPY ["src/Contracts/IntegrationBus.AccountBalance.Contracts/IntegrationBus.AccountBalance.Contracts.csproj", "src/Contracts/IntegrationBus.AccountBalance.Contracts/"]
COPY ["src/Contracts/IntegrationBus.Contracts/IntegrationBus.Contracts.csproj", "src/Contracts/IntegrationBus.Contracts/"]
COPY ["src/Contracts/IntegrationBus.Compliance.Contracts/IntegrationBus.Compliance.Contracts.csproj", "src/Contracts/IntegrationBus.Compliance.Contracts/"]
COPY ["src/Contracts/IntegrationBus.CoreLedger.Contracts/IntegrationBus.CoreLedger.Contracts.csproj", "src/Contracts/IntegrationBus.CoreLedger.Contracts/"]
COPY ["src/Contracts/IntegrationBus.SagaOrchestrator.Contracts/IntegrationBus.SagaOrchestrator.Contracts.csproj", "src/Contracts/IntegrationBus.SagaOrchestrator.Contracts/"]
RUN dotnet restore "./src/Services/IntegrationBus.SagaOrchestrator.Service/IntegrationBus.SagaOrchestrator.Service.csproj"
COPY . .
WORKDIR "/src/src/Services/IntegrationBus.SagaOrchestrator.Service"
RUN dotnet build "./IntegrationBus.SagaOrchestrator.Service.csproj" -c $BUILD_CONFIGURATION -o /app/build

# This stage is used to publish the service project to be copied to the final stage
FROM build AS publish
ARG BUILD_CONFIGURATION=Release
RUN dotnet publish "./IntegrationBus.SagaOrchestrator.Service.csproj" -c $BUILD_CONFIGURATION -o /app/publish /p:UseAppHost=false

# This stage is used in production or when running from VS in regular mode (Default when not using the Debug configuration)
FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "IntegrationBus.SagaOrchestrator.Service.dll"]