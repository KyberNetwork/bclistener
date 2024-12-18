package app

import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/KyberNetwork/evmlistener/pkg/block"
	"github.com/KyberNetwork/evmlistener/pkg/evmclient"
	"github.com/KyberNetwork/evmlistener/pkg/listener"
	"github.com/KyberNetwork/evmlistener/pkg/redis"
	"github.com/urfave/cli/v2"
	"go.uber.org/zap"
)

const (
	defaultRequestTimeout = 10 * time.Second
)

// NewApp creates a new cli App instance with common flags pre-loaded.
func NewApp() *cli.App {
	app := cli.NewApp()
	app.Flags = NewFlags()

	return app
}

func redisConfigFromCli(c *cli.Context) redis.Config {
	cfg := redis.Config{
		MasterName:   c.String(redisMasterNameFlag.Name),
		Addrs:        c.StringSlice(redisAddrsFlag.Name),
		DB:           c.Int(redisDBFlag.Name),
		KeyPrefix:    c.String(redisKeyPrefixFlag.Name),
		ReadTimeout:  c.Duration(redisReadTimeoutFlag.Name),
		WriteTimeout: c.Duration(redisWriteTimeoutFlag.Name),
	}

	if cfg.MasterName != "" {
		cfg.SentinelUsername = c.String(redisUsernameFlag.Name)
		cfg.SentinelPassword = c.String(redisPasswordFlag.Name)
	} else {
		cfg.Username = c.String(redisUsernameFlag.Name)
		cfg.Password = c.String(redisPasswordFlag.Name)
	}

	return cfg
}

// NewListener setups and returns listener service.
func NewListener(c *cli.Context) (*listener.Listener, error) {
	l := zap.S()

	httpClient := &http.Client{
		Timeout: defaultRequestTimeout,
	}
	wsRPC := c.String(wsRPCFlag.Name)
	l.Infow("Connect to node websocket rpc", "rpc", wsRPC)
	wsEVMClient, err := evmclient.DialContextWithTimeout(
		context.Background(), wsRPC, httpClient, defaultRequestTimeout)
	if err != nil {
		l.Errorw("Fail to connect to node", "rpc", wsRPC, "error", err)

		return nil, err
	}

	httpRPC := c.String(httpRPCFlag.Name)
	l.Infow("Connect to node http rpc", "rpc", httpRPC)
	httpEVMClient, err := evmclient.DialContextWithTimeout(
		context.Background(), httpRPC, httpClient, defaultRequestTimeout)
	if err != nil {
		l.Errorw("Fail to connect to node", "rpc", httpRPC, "error", err)

		return nil, err
	}

	l.Infow("Get chainID from node")
	chainID, err := httpEVMClient.ChainID(context.Background())
	if err != nil {
		l.Errorw("Fail to get chainID", "error", err)

		return nil, err
	}

	l = l.With("chainName", chainIDToName(chainID.Int64()))

	sanityCheckInterval := c.Duration(sanityCheckIntervalFlag.Name)
	var sanityEVMClient evmclient.IClient
	sanityRPC := c.String(sanityNodeRPCFlag.Name)
	if sanityRPC != "" {
		l.Infow("Connect to public node rpc for sanity check", "rpc", sanityRPC)
		sanityEVMClient, err = evmclient.DialContext(context.Background(), sanityRPC, httpClient)
		if err != nil {
			l.Errorw("Fail to setup EVM client for sanity check", "error", err)

			return nil, err
		}
	}

	redisConfig := redisConfigFromCli(c)
	redisConfigForLog := redisConfig
	redisConfigForLog.SentinelPassword = "***"
	redisConfigForLog.Password = "***"
	l.Infow("Connect to redis", "cfg", redisConfigForLog)
	redisClient, err := redis.New(redisConfig)
	if err != nil {
		l.Errorw("Fail to connect to redis", "cfg", redisConfigForLog, "error", err)

		return nil, err
	}

	maxNumBlocks := c.Int(maxNumBlocksFlag.Name)
	blockExpiration := c.Duration(blockExpirationFlag.Name)
	l.Infow("Setup new BlockKeeper", "maxNumBlocks", maxNumBlocks, "expiration", blockExpiration)
	blockKeeper := block.NewRedisBlockKeeper(l, redisClient, maxNumBlocks, blockExpiration)

	maxLen := c.Int64(publisherMaxLenFlag.Name)
	l.Infow("Setup redis stream", "maxLen", maxLen)
	redisStream := redis.NewStream(redisClient, maxLen)

	topic := c.String(publisherTopicFlag.Name)
	l.Infow("Setup handler", "topic", topic)
	handler := listener.NewHandler(l, topic, httpEVMClient, blockKeeper, redisStream,
		listener.WithEventLogs(nil, nil))

	l.Infow("Setup listener")

	return listener.New(l, wsEVMClient, httpEVMClient, handler, sanityEVMClient, sanityCheckInterval,
		listener.WithEventLogs(nil, nil)), nil
}

const (
	chainIDEthereum     = 1
	chainIDOptimism     = 10
	chainIDCronos       = 25
	chainIDBSC          = 56
	chainIDVelas        = 106
	chainIDPolygon      = 137
	chainIDSonic        = 146
	chainIDBitTorrent   = 199
	chainIDFantom       = 250
	chainIDZKSyncEra    = 324
	chainIDPolygonZKEVM = 1101
	chainIDBase         = 8453
	chainIDArbitrum     = 42161
	chainIDOasis        = 42262
	chainIDAvalanche    = 43114
	chainIDLinea        = 59144
	chainIDAurora       = 1313161554
)

//nolint:cyclop
func chainIDToName(chainID int64) string {
	switch chainID {
	case chainIDEthereum:
		return "Ethereum"
	case chainIDOptimism:
		return "Optimism"
	case chainIDCronos:
		return "Cronos"
	case chainIDBSC:
		return "BSC"
	case chainIDVelas:
		return "Velas"
	case chainIDPolygon:
		return "Polygon"
	case chainIDBitTorrent:
		return "BitTorrent"
	case chainIDFantom:
		return "Fantom"
	case chainIDArbitrum:
		return "Arbitrum"
	case chainIDOasis:
		return "Oasis"
	case chainIDAvalanche:
		return "Avalanche"
	case chainIDAurora:
		return "Aurora"
	case chainIDLinea:
		return "Linea"
	case chainIDPolygonZKEVM:
		return "Polygon zkEVM"
	case chainIDZKSyncEra:
		return "zkSync Era"
	case chainIDBase:
		return "Base"
	case chainIDSonic:
		return "Sonic"
	default:
		return strconv.FormatInt(chainID, 10)
	}
}
