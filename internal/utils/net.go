package utils

import (
	"net"
	"strconv"
	"strings"

	"github.com/bcrusu/scout/internal/errors"
	"github.com/bcrusu/scout/internal/logging"
)

var (
	logNet     = logging.New("net")
	bindAddres string
)

// returns true if the IP matches
type ipFilter func(net.IP) bool

// GetBindAddress returns the hosts bind address.
func GetBindAddress() (string, error) {
	if bindAddres != "" {
		return bindAddres, nil
	}

	ifaces, err := net.Interfaces()
	if err != nil {
		return "", errors.Wrap(err, "GetBindAddress: failed to get interfaces")
	}

	addr := getBindAddress(ifaces, ipv4Filter)
	if addr == "" {
		addr = getBindAddress(ifaces, ipv6Filter)
	}
	if addr == "" {
		return "", errors.Error("GetBindAddress: could not determine bind address")
	}

	logNet.Debugf("Using bind address %s", addr)
	bindAddres = addr
	return addr, nil
}

// JoinHostPort returns the host:port formatted string.
func JoinHostPort(addr string, port int) string {
	return net.JoinHostPort(addr, strconv.Itoa(port))
}

// SplitHostPort retruns the host, port pair.
func SplitHostPort(addr string) (string, int, error) {
	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		if strings.Contains(err.Error(), "missing port in address") {
			return addr, 0, nil
		}
		return "", 0, err
	}

	port := errors.Assert2(strconv.Atoi(portStr))
	return host, port, nil
}

// EnsureAddressPort adds the defaultPort if the addr is missing the port.
func EnsureAddressPort(addr string, defaultPort int) string {
	if host, port, err := SplitHostPort(addr); err != nil {
		// return the invalid original address
		return addr
	} else if port == 0 {
		return JoinHostPort(host, defaultPort)
	}

	return addr
}

// LookupHost resolve names to IP addresses.
func LookupHost(addr string) (string, error) {
	host, port, err := SplitHostPort(addr)
	if err != nil {
		return "", errors.Wrapf(err, "invalid address %s", addr)
	}

	addrs, err := net.LookupHost(host)
	if err != nil {
		return "", errors.Wrapf(err, "lookup failed for %s", host)
	} else if len(addrs) == 0 {
		return "", errors.Wrapf(err, "lookup result in empty for %s", host)
	}

	if port == 0 {
		return addrs[0], nil
	}

	addr = JoinHostPort(addrs[0], port)
	return addr, nil
}

func getBindAddress(ifaces []net.Interface, filter ipFilter) string {
	for _, iface := range ifaces {
		if iface.Flags&net.FlagUp == 0 ||
			iface.Flags&net.FlagLoopback != 0 ||
			iface.Flags&net.FlagPointToPoint != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			logNet.WithError(err).Error("Failed to get interface addrs.", "iface", iface)
			continue
		}

		for _, addr := range addrs {
			ip, _, err := net.ParseCIDR(addr.String())
			if err != nil {
				logNet.WithError(err).Error("Failed to parse interface address.", "iface", iface, "addr", addr)
				continue
			}

			if filter(ip) {
				return ip.String()
			}
		}
	}

	return ""
}

func ipv4Filter(ip net.IP) bool {
	return ip.To4() != nil
}

func ipv6Filter(ip net.IP) bool {
	return ip.To4() == nil
}
