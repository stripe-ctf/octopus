package harness

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/stripe-ctf/octopus/agent"
	"github.com/stripe-ctf/octopus/log"
	"github.com/stripe-ctf/octopus/sql"
	"github.com/stripe-ctf/octopus/state"
	"github.com/stripe-ctf/octopus/unix"
	"github.com/stripe-ctf/octopus/util"
	"io/ioutil"
	"math/rand"
	"net/http"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Harness struct {
	booted map[string]bool
	agents agent.List
	sql    *sql.SQL
	result chan *result
	mutex  sync.RWMutex

	nextSequenceNumber int
}

var unixClient = http.Client{
	Transport: &http.Transport{Dial: unix.Dialer},
}

type result struct {
	node  *agent.Agent
	query string
	start time.Time
	end   time.Time
	resp  []byte // unparsed response
	body  []byte // parsed response
}

func New(agents agent.List) *Harness {
	sqlPath := filepath.Join(state.Root(), "storage.sql")
	util.EnsureAbsent(sqlPath)

	return &Harness{
		result: make(chan *result, 5),
		sql:    sql.NewSQL(sqlPath),
		booted: make(map[string]bool),
		agents: agents,
	}
}

func (h *Harness) Start() {
	go h.querier()
	go h.resultHandler()
}

func (h *Harness) Stop() {
	state.WaitGroup().Exit()
}

type request struct {
	node  *agent.Agent
	query string
}

func (h *Harness) querier() {
	rng := state.NewRand("querier")
	req := make(chan *request, 0)
	go func() {
		for {
			request := <-req

			// Once we start getting non-initial queries,
			// it's time to start the query threads.
			if h.initialized() {
				// Discard the next request, in case
				// it's still an initial query (which
				// it probably is)
				<-req
				break
			}

			h.issueQuery(request.node, request.query)
			time.Sleep(10 * time.Millisecond)
		}

		threads := 4 * state.NodeCount()
		h.startQueryThreads(req, uint(threads))
	}()

	for {
		var query string
		if !h.initialized() {
			// Initialize the table
			query = h.generateInitialQuery()
		} else {
			// Set an update
			query = h.generateQuery(rng)
		}

		i := rng.Intn(len(h.agents))
		node := h.agents[i]
		req <- &request{
			node:  node,
			query: query,
		}
	}
}

func (h *Harness) startQueryThreads(req chan *request, threads uint) {
	for i := 0; i < int(threads); i++ {
		// Stagger them a little bit.
		time.Sleep(100 * time.Millisecond)
		go func() {
			for {
				request := <-req
				h.issueQuery(request.node, request.query)
			}
		}()
	}
}

func (h *Harness) initialized() bool {
	return h.NextSequenceNumber() > 0
}

func (h *Harness) NextSequenceNumber() int {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	return h.nextSequenceNumber
}

func (h *Harness) SetNextSequenceNumber(value int) {
	h.mutex.Lock()
	defer h.mutex.Unlock()
	h.nextSequenceNumber = value
}

var people = []string{"siddarth", "gdb", "christian", "andy", "carl"}

func (h *Harness) generateInitialQuery() string {
	peopleFmt := `("` + strings.Join(people, "\"), (\"") + `")`
	query := fmt.Sprintf(`
CREATE TABLE ctf3
(name STRING PRIMARY KEY,
friendCount INT DEFAULT 0,
requestCount INT DEFAULT 0,
favoriteWord CHAR(15) DEFAULT "");
INSERT INTO ctf3 (name) VALUES %s;`,
		peopleFmt)
	return strings.TrimLeft(query, "\n")
}

func (h *Harness) generateQuery(rng *rand.Rand) string {
	amount := rng.Intn(100) + 1
	i := rng.Intn(len(people))
	person := people[i]
	word := state.RandomString(rng, 15)
	query := fmt.Sprintf(`UPDATE ctf3 SET friendCount=friendCount+%d, requestCount=requestCount+1, favoriteWord="%s" WHERE name="%s"; SELECT * FROM ctf3;`, amount, word, person)
	return query
}

func (h *Harness) nodeBooted(node *agent.Agent) bool {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	_, ok := h.booted[node.Name]
	return ok
}

func (h *Harness) setNodeBooted(node *agent.Agent) {
	h.mutex.RLock()
	defer h.mutex.RUnlock()
	h.booted[node.Name] = true
}

func (h *Harness) issueQuery(node *agent.Agent, query string) {
	log.Debugf("[harness] Making request to %v: %#v", node, query)

	b := bytes.NewBufferString(query)
	url := node.ConnectionString + "/sql"

	start := time.Now()
	resp, err := unixClient.Post(url, "application/octet-stream", b)
	end := time.Now()

	if err != nil {
		if h.nodeBooted(node) {
			log.Printf("[harness] Sleeping 100ms after request error from %s (in response to %#v): %s", node, query, err)
		}
		time.Sleep(100 * time.Millisecond)
		return
	}

	body, err := ioutil.ReadAll(resp.Body)
	resp.Body.Close()

	if err != nil {
		log.Printf("[harness] Error reading body from %v (in response to %#v): %s", node, query, err)
	}

	if resp.StatusCode != 200 {
		log.Printf("[harness] Sleeping 100ms after HTTP %d status code from %s (in response to %#v): %s", resp.StatusCode, node, query, body)
		time.Sleep(100 * time.Millisecond)
		return
	}

	log.Debugf("[harness] Received response to %v (%#v): %s", node, query, body)

	h.setNodeBooted(node)

	h.result <- &result{
		node:  node,
		query: query,
		start: start,
		end:   end,
		resp:  body,
	}
}

// Rules for sequence numbers:
//
// - Gaps are temporarily OK, but not in the long run.
// - TODO: make sure there isn't an attack of waiting a long time to respond to the
//   first request
func (h *Harness) resultHandler() {
	results := make(map[int]*result)
	for {
		result := <-h.result
		sequenceNumber, body, err := h.parseResponse(result.resp)
		if err != nil {
			h.lose(err.Error())
			return
		}
		result.body = body

		nextSequenceNumber := h.NextSequenceNumber()
		if sequenceNumber < nextSequenceNumber {
			h.losef(`[%d] Received an already-processed sequence number from %v in response to %s

Output: %s`, sequenceNumber, result.node, result.query, util.FmtOutput(result.resp))
			return
		}

		if old, ok := results[sequenceNumber]; ok {
			h.losef(`[%d] Received a still-pending sequence number from %v in response to %s

Output: %s

This sequence number was originally received in response to %s

Original output: %s`, sequenceNumber, result.node, result.query, util.FmtOutput(result.resp), old.query, util.FmtOutput(old.resp))
			return
		}

		if sequenceNumber > nextSequenceNumber {
			log.Printf("[%d] Result from %v waiting on sequence number %d", sequenceNumber, result.node, nextSequenceNumber)
		}

		results[sequenceNumber] = result
		h.processPending(results, nextSequenceNumber)
	}
}

func (h *Harness) processPending(results map[int]*result, nextSequenceNumber int) {
	for {
		result, ok := results[nextSequenceNumber]
		if !ok {
			return
		}

		output, err := h.sql.Execute("harness", result.query)
		if err != nil {
			h.losef("[%d] Could not execute statement that %v claimed was fine: %s", nextSequenceNumber, result.node, err)
		}

		if !bytes.Equal(result.body, output.Stdout) {
			h.losef(`[%d] Received incorrect output from %v for query %s

Output: %s

Correct output: %s`, nextSequenceNumber, result.node, result.query, util.FmtOutput(result.body), util.FmtOutput(output.Stdout))
		} else {
			state.RecordCorrectQuery()
			log.Printf(`[harness] [%d] Received correct output from %v for query %s

Output: %s`, nextSequenceNumber, result.node, result.query, util.FmtOutput(result.body))
		}

		// Update bookkeeping
		delete(results, nextSequenceNumber)
		nextSequenceNumber += 1
		h.SetNextSequenceNumber(nextSequenceNumber)
	}
}

func (h *Harness) lose(msg string) {
	h.losef("%s", msg)
}

func (h *Harness) losef(msg string, v ...interface{}) {
	disqualifier := fmt.Sprintf(msg, v...)
	state.RecordDisqualifier(disqualifier)
	state.WaitGroup().Exit()
}

var matcher *regexp.Regexp = regexp.MustCompile("^(?s)SequenceNumber: (\\d+)\n(.*)$")

func (h *Harness) parseResponse(res []byte) (int, []byte, error) {
	matches := matcher.FindSubmatch(res)
	if matches != nil {
		sequenceNumber, err := strconv.Atoi(string(matches[1]))
		return sequenceNumber, matches[2], err
	} else {
		msg := fmt.Sprintf("Could not parse response: %#v", string(res))
		return 0, nil, errors.New(msg)
	}
}
