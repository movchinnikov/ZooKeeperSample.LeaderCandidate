using ZooKeeperSample.Infrastructure;

namespace ZookeeperSample.LeaderCandidate;

public sealed class Worker
{
    private readonly LeaderElectionService _zooKeeperClient;
    
    private Guid Id { get; }
    
    public Worker(LeaderElectionService zooKeeperClient)
    {
        _zooKeeperClient = zooKeeperClient;
        Id = Guid.NewGuid();
    }
    
    public async Task ExecuteAsync(CancellationToken token)
    {
        var serviceName = $"candidate-{Id}";
        Console.WriteLine($"Hi, I am {serviceName}");
        
        while (!token.IsCancellationRequested)
        {
            if (await _zooKeeperClient.IsLeaderAsync(serviceName))
                Console.WriteLine($"[{serviceName}] [{DateTime.Now}]: I did something useful");
            else 
                Console.WriteLine($"[{serviceName}] [{DateTime.Now}]: Sorry, I did nothing");
            await Task.Delay(10000, token);
        }
    }
}

public sealed class Program
{
    public static async Task Main(string[] args)
    {
        var instance = new Worker(new WorkerLeaderElectionService(new ZooKeeperClientService()));

        await instance.ExecuteAsync(CancellationToken.None);
    }
}