FROM mcr.microsoft.com/dotnet/runtime:7.0 AS base
WORKDIR /app

FROM mcr.microsoft.com/dotnet/sdk:7.0 AS build
WORKDIR /src
COPY ["ZookeeperSample.LeaderCandidate/ZookeeperSample.LeaderCandidate.csproj", "ZookeeperSample.LeaderCandidate/"]
RUN dotnet restore "ZookeeperSample.LeaderCandidate/ZookeeperSample.LeaderCandidate.csproj"
COPY . .
WORKDIR "/src/ZookeeperSample.LeaderCandidate"
RUN dotnet build "ZookeeperSample.LeaderCandidate.csproj" -c Release -o /app/build

FROM build AS publish
RUN dotnet publish "ZookeeperSample.LeaderCandidate.csproj" -c Release -o /app/publish

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "ZookeeperSample.LeaderCandidate.dll"]
