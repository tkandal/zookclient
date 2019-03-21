package zookclient

import (
    "fmt"
    "github.com/kikinteractive/curator-go"
    "github.com/samuel/go-zookeeper/zk"
    "log"
    "sync"
)

type StateType int

const (
    Latent StateType = iota
    Started
    Closed
)

type PersistentNode struct {
    client                  curator.CuratorFramework
    mode                    curator.CreateMode
    useProtection           bool
    basePath                string
    data                    []byte
    state                   StateType
    backgroundCallback      curator.BackgroundCallback
    authFailure             bool
    nodePath                string
    wg                      sync.WaitGroup
    connectionStateListener curator.ConnectionStateListener
    watcher                 curator.Watcher
    checkExistsCallback     curator.BackgroundCallback
    setDataCallback         curator.BackgroundCallback
    lock                    sync.Mutex
}

func NewPersistentNode(client curator.CuratorFramework, mode curator.CreateMode, protection bool, basePath string, initData []byte) *PersistentNode {
    data := make([]byte, len(initData))
    copy(data, initData)
    pNode := &PersistentNode{
        client:        client,
        mode:          mode,
        useProtection: protection,
        basePath:      basePath,
        data:          data,
        lock:          sync.Mutex{},
    }
    pNode.state = Latent

    pNode.backgroundCallback = curator.BackgroundCallback(
        func(client curator.CuratorFramework, ev curator.CuratorEvent) error {
            if pNode.state == Started {
                pNode.processBackgroundCallback(ev)
            } else {
                pNode.processBackgroundCallbackClosedState(ev)
            }
            return nil
        })

    pNode.authFailure = false
    pNode.wg.Add(1)

    pNode.connectionStateListener = curator.NewConnectionStateListener(
        func(client curator.CuratorFramework, newState curator.ConnectionState) {
            if newState == curator.RECONNECTED {
                pNode.createNode()
            }
        })

    pNode.watcher = curator.NewWatcher(
        func(ev *zk.Event) {
            if ev.Type.String() == curator.DELETE.String() {
                pNode.createNode()
            } else if ev.Type.String() == curator.SET_DATA.String() {
                pNode.watchNode()
            }
        })

    pNode.checkExistsCallback = curator.BackgroundCallback(
        func(client curator.CuratorFramework, ev curator.CuratorEvent) error {
            if ev.Err() == curator.ErrNoNode {
                pNode.createNode()
            } else {
                if ev.Stat().EphemeralOwner != curator.EPHEMERAL {
                    log.Printf("Existing node ephemeral state doesn't match requested state. Maybe the node was created outside of PersistentNode? %s", pNode.basePath)
                }
            }
            return nil
        })

    pNode.setDataCallback = curator.BackgroundCallback(
        func(client curator.CuratorFramework, ev curator.CuratorEvent) error {
            if ev.Err() == curator.ErrNothing {
                // Initialisation complete
                pNode.wg.Done()
            }
            return nil
        })
    return pNode
}

func (p *PersistentNode) Start() error {
    p.lock.Lock()
    defer p.lock.Unlock()
    if p.state == Started {
        return fmt.Errorf("already started")
    }
    p.client.ConnectionStateListenable().AddListener(p.connectionStateListener)
    p.createNode()
    p.state = Started
    return nil
}

func (p *PersistentNode) Close() error {
    p.lock.Lock()
    defer p.lock.Unlock()
    if p.state != Started {
        return fmt.Errorf("not started")
    }
    p.client.ConnectionStateListenable().RemoveListener(p.connectionStateListener)
    if err := p.deleteNode(); err != nil {
        return err
    }
    p.state = Closed
    return nil
}

func (p *PersistentNode) ActualPath() string {
    return p.nodePath
}

func (p *PersistentNode) SetData(d []byte) error {
    if d == nil || len(d) == 0 {
        return fmt.Errorf("data is empty")
    }
    p.lock.Lock()
    defer p.lock.Unlock()
    if len(p.nodePath) == 0 {
        return fmt.Errorf("initial create has not been processed")
    }
    c := make([]byte, len(d))
    copy(c, d)
    p.data = c
    if p.isActive() {
        if _, err := p.client.SetData().InBackground().ForPathWithData(p.nodePath, p.data); err != nil {
            return err
        }
    }
    return nil
}

func (p *PersistentNode) GetData() []byte {
    return p.data
}

func (p *PersistentNode) IsAuthFailure() bool {
    return p.authFailure
}

func (p *PersistentNode) processBackgroundCallbackClosedState(event curator.CuratorEvent) {
    path := ""
    if event.Err() == curator.ErrNodeExists {
        path = event.Path()
    } else if event.Err() == curator.ErrNothing {
        path = event.Name()
    }
    if len(path) > 0 {
        if err := p.client.Delete().DeletingChildrenIfNeeded().InBackground().ForPath(path); err != nil {
            log.Printf("delete %s failed, error = %v", path, err)
        }
    }
}

func (p *PersistentNode) processBackgroundCallback(event curator.CuratorEvent) {
    path := ""
    nodeExists := false

    if event.Err() == curator.ErrNodeExists {
        path = event.Path()
        nodeExists = true
    } else if event.Err() == curator.ErrNothing {
        path = event.Name()
    } else if event.Err() == curator.ErrNoAuth {
        log.Printf("client does not have authorisation to write node at path %s", event.Path())
        p.authFailure = true
        return
    }
    if len(path) > 0 {
        p.authFailure = false
        p.lock.Lock()
        p.nodePath = path
        p.lock.Unlock()
        p.watchNode()
        if nodeExists {
            p.client.SetData().InBackgroundWithCallback(p.setDataCallback).ForPathWithData(p.nodePath, p.data)
        } else {
            // Initialisation complete
            p.wg.Done()
        }
    } else {
        p.createNode()
    }
}

func (p *PersistentNode) watchNode() {
    if !p.isActive() {
        return
    }
    if len(p.nodePath) > 0 {
        p.client.CheckExists().UsingWatcher(p.watcher).InBackgroundWithCallback(p.checkExistsCallback).ForPath(p.nodePath)
    }
}

func (p *PersistentNode) createNode() {
    if !p.isActive() {
        return
    }
    existingPath := p.nodePath
    createPath := p.basePath
    if len(existingPath) > 0 && !p.useProtection {
        createPath = existingPath
    }
    createBuilder := p.client.Create().CreatingParentsIfNeeded()
    str, err := createBuilder.WithMode(p.getCreateMode(len(existingPath) > 0)).InBackgroundWithCallback(p.backgroundCallback).ForPathWithData(createPath, p.data)
    if err != nil {
        log.Printf("create %s in background failed, error = %v", str, err)
    }
}

func (p *PersistentNode) getCreateMode(isPathSet bool) curator.CreateMode {
    m := p.mode
    if isPathSet {
        switch p.mode {
        case curator.EPHEMERAL_SEQUENTIAL:
            return curator.EPHEMERAL
        case curator.PERSISTENT_SEQUENTIAL:
            return curator.PERSISTENT
        default:
            return m
        }
    }
    return m
}

func (p *PersistentNode) deleteNode() error {
    if len(p.nodePath) > 0 {
        return p.client.Delete().DeletingChildrenIfNeeded().ForPath(p.nodePath)
    }
    return nil
}

func (p *PersistentNode) isActive() bool {
    return p.state == Started
}
