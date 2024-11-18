package utils

import (
	"net"
	"strconv"

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
		return "", errors.Wrap(err, "failed to get interfaces")
	}

	addr := getBindAddress(ifaces, ipv4Filter)
	if addr == "" {
		addr = getBindAddress(ifaces, ipv6Filter)
	}
	if addr == "" {
		return "", errors.Error("could not determine bind address")
	}

	logNet.Infof("Using bind address=%s", addr)
	bindAddres = addr
	return addr, nil
}

// JoinHostPort returns the host:port formatted string.
func JoinHostPort(addr string, port int) string {
	return net.JoinHostPort(addr, strconv.Itoa(port))
}

// LookupHosts resolve names to IP addresses.
func LookupHosts(addrs ...string) ([]string, error) {
	result := make([]string, len(addrs))

	for i, addr := range addrs {
		host, port, err := net.SplitHostPort(addr)
		if err != nil {
			return nil, errors.Wrapf(err, "invalid address %s", addr)
		}

		addrs, err := net.LookupHost(host)
		if err != nil {
			return nil, errors.Wrapf(err, "lookup failed for %s", host)
		} else if len(addrs) == 0 {
			return nil, errors.Wrapf(err, "lookup result in empty for %s", host)
		}

		result[i] = net.JoinHostPort(addrs[0], port)
	}

	return result, nil
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
