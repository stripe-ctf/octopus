package director

import (
	"flag"
	"github.com/stripe-ctf/octopus/agent"
	"github.com/stripe-ctf/octopus/exit"
	"github.com/stripe-ctf/octopus/harness"
	"github.com/stripe-ctf/octopus/log"
	"github.com/stripe-ctf/octopus/network"
	"github.com/stripe-ctf/octopus/state"
	"math/rand"
	"time"
)

// Apologies to Netflix and Chaos Monkey
type MonkeyConfig struct {
	enabled           bool
	offset, frequency time.Duration
}

// These numbers were scientifically generated by thinking really hard for the
// better part of two seconds.
var monkeys = map[string]*MonkeyConfig{
	"latency":      &MonkeyConfig{true, 0 * time.Second, 500 * time.Millisecond},
	"jitter":       &MonkeyConfig{true, 0 * time.Second, 1 * time.Second},
	"lagsplit":     &MonkeyConfig{true, 3 * time.Second, 2 * time.Second},
	"link":         &MonkeyConfig{true, 6 * time.Second, 3 * time.Second},
	"netsplit":     &MonkeyConfig{true, 9 * time.Second, 6 * time.Second},
	"unneighborly": &MonkeyConfig{true, 9 * time.Second, 9 * time.Second},
	"freeze":       &MonkeyConfig{true, 12 * time.Second, 6 * time.Second},
	"murder":       &MonkeyConfig{false, 20 * time.Second, 10 * time.Second},
	"spof":         &MonkeyConfig{true, 1 * time.Second, 0 * time.Second},
}

func init() {
	for monkey, config := range monkeys {
		flag.BoolVar(&config.enabled, "enable-"+monkey+"-monkey",
			config.enabled, "Enable the "+monkey+" monkey")
	}
}

type Director struct {
	g       *exit.WaitGroup
	net     *network.Network
	config  map[string]*MonkeyConfig
	agents  agent.List
	harness *harness.Harness
}

func NewDirector() *Director {
	agents := agent.NewList()

	return &Director{
		agents:  agents,
		net:     network.New(),
		config:  monkeys,
		harness: harness.New(agents),
	}
}

func (d *Director) Dryrun() {
	d.agents.Dryrun()
}

func (d *Director) Start() {
	rng := state.NewRand("director")
	d.harness.Start()

	// Give a 100ms grace period for startup
	time.Sleep(100 * time.Millisecond)

	// Set up directories
	d.agents.Prepare()

	// Populate initial link latencies
	for _, link := range d.net.Links() {
		link.SetLatency(d.makeLatency(rng, 1))
		link.SetJitter(d.makeJitter(rng, 1))
	}
	d.net.Start()
	for monkey, config := range monkeys {
		if config.enabled {
			go d.spawn(monkey)
		}
	}
	d.agents.Start()
}

func (d *Director) makeLatency(rng *rand.Rand, intensity float64) uint {
	l := int((20 + rng.NormFloat64()*10) * intensity)
	if l < 1 {
		return 1
	} else {
		return uint(l)
	}
}
func (d *Director) makeJitter(rng *rand.Rand, intensity float64) uint {
	return uint(rng.Intn(int(20 * intensity)))
}
func (d *Director) makeDuration(rng *rand.Rand, scale, intensity float64) time.Duration {
	t := int(scale*intensity) + 1
	return time.Duration(rng.Intn(t)) * time.Millisecond
}

func (d *Director) randomLink(rng *rand.Rand) *network.Link {
	links := d.net.Links()
	choice := rng.Intn(len(links))
	return links[choice]
}

func (d *Director) randomPartition(rng *rand.Rand) []*network.Link {
	count := state.NodeCount()

	// If there are fewer than three nodes, netsplits are pretty
	// boring
	if count < 3 {
		return []*network.Link{d.net.Links()[0]}
	}
	perm := rng.Perm(count)
	splitPoint := rng.Intn(count-2) + 1
	split := make([]uint, 0)
	for i := 0; i < splitPoint; i++ {
		split = append(split, uint(perm[i]))
	}
	log.Debugf("Made a netsplit: %v", split)
	return d.net.FindPerimeter(split)
}

// Choose random connections such that each node has a connection in the
// returned list.
func (d *Director) randomNeighborLinks(rng *rand.Rand) []*network.Link {
	links := d.net.Links()
	perm := rng.Perm(len(links))

	cover := make(map[uint]bool, state.NodeCount())
	for i := 0; i < state.NodeCount(); i++ {
		cover[uint(i)] = false
	}
	out := make([]*network.Link, 0, (state.NodeCount()+1)/2)

	for _, i := range perm {
		link := links[i]
		a1, a2 := link.Agents()
		if !cover[a1] || !cover[a2] {
			out = append(out, link)
			cover[a1] = true
			cover[a2] = true
		}
	}
	return out
}

func (d *Director) randomAgent(rng *rand.Rand) *agent.Agent {
	choice := rng.Intn(len(d.agents))
	return d.agents[choice]
}
