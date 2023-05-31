using ZooKeeperSample.Infrastructure;

namespace ZookeeperSample.LeaderCandidate;

public sealed class WorkerLeaderElectionService : LeaderElectionService
{
    protected override string ServiceGroupName => "WorkerService";

    public WorkerLeaderElectionService(
        ZooKeeperClientService? zooKeeperClientService)
        : base(zooKeeperClientService)
    {
    }
}