package config

import (
	"fmt"
	"net"
	"net/url"
	"os"
)

// Local fixture acquisition remains unchanged. Non-loopback acquisition must
// explicitly select the bounded Mainnet environment, even outside Compose.
func loadExecutionSafety(c Config, x *IndexingConfig) error {
	environment := os.Getenv("EXTRACTOR_ENVIRONMENT")
	u, err := url.Parse(x.RPC.URL)
	if err != nil || u == nil {
		return fmt.Errorf("invalid RPC endpoint")
	}
	loopback := u.Hostname() == "localhost"
	if ip := net.ParseIP(u.Hostname()); ip != nil {
		loopback = ip.IsLoopback()
	}
	if environment == "" || environment == "local" {
		if !loopback {
			return fmt.Errorf("non-loopback acquisition requires EXTRACTOR_ENVIRONMENT=mainnet")
		}
		if os.Getenv("EXTRACTOR_MAX_RPC_REQUESTS") != "" || os.Getenv("EXTRACTOR_RUN_TIMEOUT") != "" || os.Getenv("EXTRACTOR_MAINNET_OPT_IN") != "" {
			return fmt.Errorf("execution safety settings require mainnet environment")
		}
		return nil
	}
	if environment != "mainnet" {
		return fmt.Errorf("invalid EXTRACTOR_ENVIRONMENT")
	}
	if os.Getenv("EXTRACTOR_MAINNET_OPT_IN") != "true" || c.ChainID != "1" || (x.Mode != "index" && x.Mode != "reindex") || x.Options.End == nil || u.Scheme != "https" {
		return fmt.Errorf("mainnet requires explicit opt-in, chain 1, HTTPS and bounded index mode")
	}
	x.RPC.MaxRequests, err = uintSetting("EXTRACTOR_MAX_RPC_REQUESTS", 0, true)
	if err != nil || x.RPC.MaxRequests == 0 {
		return fmt.Errorf("invalid EXTRACTOR_MAX_RPC_REQUESTS")
	}
	if os.Getenv("EXTRACTOR_RUN_TIMEOUT") == "" {
		return fmt.Errorf("EXTRACTOR_RUN_TIMEOUT is required")
	}
	x.RunTimeout, err = durationSetting("EXTRACTOR_RUN_TIMEOUT", 0)
	return err
}
