package zookclient;

import (
    "encoding/json"
    "github.com/kikinteractive/curator-go"
    "github.com/pkg/errors"
    "github.com/samuel/go-zookeeper/zk"
    "log"
    "time"
)

type ServiceType int8

const (
    CORE ServiceType = iota
    ADAPTER
    LIVE_ADAPTER
)

type LiveNodeInfo struct {
    Name      string    `json:"name"`
    StartTime time.Time `json:"startTime"`
    Host      string    `json:"host"`
    JMXPort   int       `json:"jmxPort"`
    StatusURL string    `json:"statusUrl"`
}

type NodeInfo struct {
    Name          string            `json:"name"`
    Properties    map[string]string `json:"properties"`
    Type          ServiceType       `json:"type"`
    LastStartTime time.Time         `json:"lastStartTime"`
    LastExitTime  time.Time         `json:"lastExitTime"`
}

type ZooKeeperClient struct {
    curator     curator.CuratorFramework
    retryPolicy *curator.ExponentialBackoffRetry
}

func NewZooKeeperClient(connStr string) (*ZooKeeperClient, error) {
    log.Printf("connecting to %s", connStr)
    rp := curator.NewExponentialBackoffRetry(time.Second, 512, 15*time.Second)
    c := curator.NewClient(connStr, rp)
    if err := c.Start(); err != nil {
        return nil, errors.Wrap(err, "could not start zookeeper-client")
    }
    if err := createRoot(c); err != nil {
        return nil, errors.Wrap(err, "could not create root")
    }
    return &ZooKeeperClient{curator: c, retryPolicy: rp}, nil
}

func (z *ZooKeeperClient) GetChildren(path string) ([]string, error) {
    var err error
    strs := make([]string, 0)
    is, err := exists(z.curator, path)
    if !is || err != nil {
        return nil, err
    }
    strs, err = z.curator.GetChildren().ForPath(path)
    if err != nil {
        return nil, err
    }
    return strs, nil
}

func (z *ZooKeeperClient) CreatePath(path string) error {
    _, err := z.curator.Create().CreatingParentsIfNeeded().ForPath(path)
    return err
}

func (z *ZooKeeperClient) GetNode(path string) (interface{}, error) {
    b, err := z.curator.GetData().ForPath(path)
    if err != nil {
        return nil, err
    }
    obj := new(interface{})
    if err = json.Unmarshal(b, &obj); err != nil {
        return nil, err
    }
    return obj, nil
}

func (z *ZooKeeperClient) GetNodes(path string) ([]interface{}, error) {
    objs := make([]interface{}, 0)
    strs, err := z.GetChildren(path)
    if err != nil {
        return objs, err
    }
    for _, s := range strs {
        nodePath := curator.JoinPath(path, s)
        b, err := z.curator.GetData().ForPath(nodePath)
        if err != nil {
            return objs, err
        }
        if b != nil {
            var o interface{}
            if err = json.Unmarshal(b, &o); err != nil {
                return objs, err
            }
            objs = append(objs, o)
        }
    }
    return objs, nil
}

func (z *ZooKeeperClient) Exists(path string) bool {
    is, err := exists(z.curator, path)
    return is && err == nil
}

func (z *ZooKeeperClient) SetObject(path string, obj interface{}) error {
    b, err := json.Marshal(obj)
    if err != nil {
        return err
    }
    return z.SetByte(path, b)
}

func (z *ZooKeeperClient) SetByte(path string, b []byte) error {
    return z.SetData(path, b, curator.PERSISTENT)
}

func (z *ZooKeeperClient) SetData(path string, data []byte, mode curator.CreateMode) error {
    var err error
    is, err := exists(z.curator, path)
    if !is || err != nil {
        if _, err = z.curator.Create().WithMode(mode).CreatingParentsIfNeeded().ForPathWithData(path, data); err != nil {
            return err
        }
    } else {
        if _, err = z.curator.SetData().ForPathWithData(path, data); err != nil {
            return err
        }
    }
    return nil
}

func (z *ZooKeeperClient) GetData(path string) ([]byte, error) {
    return z.curator.GetData().ForPath(path)
}

func (z *ZooKeeperClient) GetStringData(path string) (string, error) {
    b, err := z.GetData(path)
    if err != nil {
        return "", err
    }
    return string(b), nil
}

func (z *ZooKeeperClient) Delete(path string, delChildren bool) error {
    is, err := exists(z.curator, path)
    if !is || err != nil {
        return nil
    } else {
        delBuilder := z.curator.Delete()
        if delChildren {
            return delBuilder.DeletingChildrenIfNeeded().ForPath(path)
        } else {
            return delBuilder.ForPath(path)
        }
    }
}

func (z *ZooKeeperClient) CreateNode(path string, data []byte) error {
    return z.SetData(path, data, curator.PERSISTENT)
}

func (z *ZooKeeperClient) CreateEphemeralNode(path string, data []byte) error {
    if z.Exists(path) {
        if err := z.curator.Delete().ForPath(path); err != nil {
            return err
        }
    }
    if err := z.SetData(path, data, curator.EPHEMERAL); err != nil {
        return err
    }
    return nil
}

func (z *ZooKeeperClient) CreatePersistentNode(path string, obj interface{}) (interface{}, error) {
    b, err := json.Marshal(obj)
    if err != nil {
        return nil, err
    }
    p, err := z.curator.Create().WithMode(curator.EPHEMERAL).CreatingParentsIfNeeded().ForPathWithData(path, b)
    if err != err {
        return nil, err
    }
    myWatcher := curator.NewWatcher(func(ev *zk.Event) {
        for {
            switch ev.Type.String() {
            case "EventNodeDeleted":
                p, err = z.curator.Create().WithMode(curator.EPHEMERAL).CreatingParentsIfNeeded().ForPathWithData(path, b)
                if err != nil {
                    log.Printf("recreate %s failed\n", ev.Path)
                }
            case "EventNodeCreated":
                log.Printf("created %s\n", ev.Path)
            case "EventNodeDataChanged":
                log.Printf("changed %s\n", ev.Path)
            case "EventNodeChildrenChanged":
                log.Printf("children changed %s\n", ev.Path)
            case "EventSession":
                log.Printf("session error %s\n", ev.Path)
                return
            case "EventNotWatching":
                log.Printf("not watching error %s\n", ev.Path)
                return
            }
        }
    })
    if _, err = z.curator.CheckExists().Watched().UsingWatcher(myWatcher).ForPath(p); err != nil {
        return nil, err
    }
    return nil, nil
}

func (z *ZooKeeperClient) Close() error {
    err := z.curator.Close()
    z.curator = nil
    z.retryPolicy = nil
    return err
}

func createRoot(c curator.CuratorFramework) error {
    var err error
    is, err := exists(c, "/")
    if !is || err != nil {
        if _, err = c.Create().WithMode(curator.PERSISTENT).CreatingParentsIfNeeded().ForPath("/"); err != nil {
            return err
        }
    }
    return nil
}

func exists(c curator.CuratorFramework, str string) (bool, error) {
    stat, err := c.CheckExists().ForPath(str)
    return stat != nil, err
}
