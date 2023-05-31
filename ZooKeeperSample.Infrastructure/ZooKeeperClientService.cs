using System.Text;
using org.apache.zookeeper;
using org.apache.zookeeper.data;

namespace ZooKeeperSample.Infrastructure;

public sealed class ZooKeeperClientService : IAsyncDisposable
{
    private bool _disposed;

    private ZooKeeper? _zooKeeper;

    public void Connect(Watcher watcher)
    {
        try
        {
            _zooKeeper = new ZooKeeper("zoo1:2181,zoo2:2181,zoo3:2181", 10000, watcher);
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
            throw;
        }
    }

    public async Task<string> GetLeaderMetadataAsync(string servicePath)
    {
        if (_zooKeeper == null)
        {
            throw new KeeperException.ConnectionLossException();
        }
        
        var childNodes =
            (await _zooKeeper!.getChildrenAsync(servicePath)).Children.OrderBy(x => x);

        var leadChild = await _zooKeeper.getDataAsync($"{servicePath}/{childNodes.First()}", true);
        var leaderData = Encoding.UTF8.GetString(leadChild.Data);

        return leaderData;
    }

    public async Task EnsureCreateNodeAsync(string path, string data, List<ACL> listAcl, CreateMode createMode)
    {
        if (_zooKeeper == null)
        {
            throw new KeeperException.ConnectionLossException();
        }
        
        var rootNode = await _zooKeeper.existsAsync(path);
        if (rootNode == null)
        {
            try
            {
                await CreateNodeAsync(path, data, listAcl, createMode);
            }
            catch (KeeperException.NodeExistsException)
            {
                // ignored
            }
        }
    }
    
    public async Task CreateNodeAsync(string path, string data, List<ACL> listAcl, CreateMode createMode)
    {
        if (_zooKeeper == null)
        {
            throw new KeeperException.ConnectionLossException();
        }
        
        await _zooKeeper!.createAsync(
            path,
            Encoding.UTF8.GetBytes(data),
            listAcl,
            createMode);
    } 

    private async Task DisposeAsync(bool disposing)
    {
        if (!_disposed)
        {
            if (disposing)
            {
                await _zooKeeper?.closeAsync()!;
            }

            _disposed = true;
        }
    }
    
    public async ValueTask DisposeAsync()
    {
        await DisposeAsync(true);
        
        GC.SuppressFinalize(this);
    }
}