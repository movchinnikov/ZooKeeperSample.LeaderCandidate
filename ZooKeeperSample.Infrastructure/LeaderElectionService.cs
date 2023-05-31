using org.apache.zookeeper;

namespace ZooKeeperSample.Infrastructure;

public abstract class LeaderElectionService : Watcher
{
    private const string LeaderElectionPath = "/ELECTION";
    
    private readonly ZooKeeperClientService _zooKeeperClientService;
    private readonly IDictionary<string, bool> _leadersMap;
    
    private bool _leaderCheckReady = false;
    private bool _serviceCheckReady = false;
    
    protected abstract string ServiceGroupName { get; }
    
    protected LeaderElectionService(
        ZooKeeperClientService? zooKeeperClientService)
    {
        _zooKeeperClientService =
            zooKeeperClientService ?? throw new ArgumentNullException(nameof(zooKeeperClientService)); 
        _zooKeeperClientService.Connect(this);
        _leadersMap = new Dictionary<string, bool>(); // in this we maintain our last know status
    }

    public async Task<bool> IsLeaderAsync(string serviceName)
    {
        if (_leadersMap.Any() && _leadersMap.TryGetValue(serviceName, out var result))
        {
            return result;
        }
        return await CheckLeaderAsync(serviceName);
    }

    public async Task<bool> CheckLeaderAsync(string serviceName)
    {
        try
        {
            if (!_leaderCheckReady)
            {
                await AddRootNode();
            }
            
            var servicePath = $"{LeaderElectionPath}/{ServiceGroupName}";
            
            if (!_serviceCheckReady)
            {
                await AddServicePath(servicePath);
                await AddCounterPath(servicePath, serviceName);
            }
            
            var leaderData = await _zooKeeperClientService.GetLeaderMetadataAsync(servicePath);

            _leadersMap[serviceName] = leaderData == serviceName;

            return _leadersMap[serviceName];
        }
        catch (Exception)
        {
            return false;
        }
    }

    private async Task AddRootNode()
    {
        await _zooKeeperClientService.EnsureCreateNodeAsync(
            path: LeaderElectionPath,
            data: LeaderElectionPath,
            listAcl: ZooDefs.Ids.OPEN_ACL_UNSAFE,
            createMode: CreateMode.PERSISTENT);

        _leaderCheckReady = true;
    }

    private async Task AddServicePath(string servicePath)
    {
        await _zooKeeperClientService.EnsureCreateNodeAsync(
            path: servicePath,
            data: servicePath,
            listAcl: ZooDefs.Ids.OPEN_ACL_UNSAFE,
            createMode: CreateMode.PERSISTENT);
    }
    
    private async Task AddCounterPath(string servicePath, string serviceName)
    {
        await _zooKeeperClientService.CreateNodeAsync(
            $"{servicePath}/n_",
            serviceName,
            ZooDefs.Ids.OPEN_ACL_UNSAFE,
            CreateMode.EPHEMERAL_SEQUENTIAL);
        _serviceCheckReady = true;
    }


    public override async Task process(WatchedEvent @event)
    {
        switch (@event.get_Type())
        {
            case Event.EventType.NodeDeleted:
                _leadersMap.Clear();
                break;
            case Event.EventType.None:
                switch (@event.getState())
                {
                    case Event.KeeperState.SyncConnected:
                        await AddRootNode();
                        break;
                    case Event.KeeperState.Disconnected:
                        _leaderCheckReady = false;
                        _leadersMap.Clear();
                        _zooKeeperClientService.Connect(this);
                        break;
                }
                break;
        }
    }
}