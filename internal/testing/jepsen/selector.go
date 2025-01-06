package jepsen

import (
	"math"
	"math/rand/v2"

	"github.com/bcrusu/scout/internal/testing/agent"
	"github.com/bcrusu/scout/internal/utils"
)

type nodeSelector func([]*node) []*node

func compose(selectors ...nodeSelector) nodeSelector {
	return func(nodes []*node) []*node {
		for _, selector := range selectors {
			nodes = selector(nodes)
			if len(nodes) == 0 {
				return nil
			}
		}
		return nodes
	}
}

func selectType(types ...agent.ServiceType) nodeSelector {
	set := utils.MakeSet(types)

	return func(nodes []*node) []*node {
		var result []*node
		for _, node := range nodes {
			if set[node.Type] {
				result = append(result, node)
			}
		}

		return result
	}
}

func selectRand(count int) nodeSelector {
	return func(nodes []*node) []*node {
		if count >= len(nodes) {
			return nodes
		}

		perm := rand.Perm(len(nodes))
		result := make([]*node, count)

		for i := range count {
			result[i] = nodes[perm[i]]
		}

		return result
	}
}

func selectFraction(fraction float64) nodeSelector {
	return func(nodes []*node) []*node {
		count := int(math.Round(float64(len(nodes)) * fraction))
		return selectRand(count)(nodes)
	}
}
