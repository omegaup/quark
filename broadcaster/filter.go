package broadcaster

import (
	"fmt"
	"strings"
)

// Filter is used to determine whether messages should be sent to a particular
// subscriber.
type Filter interface {
	String() string
	Matches(msg *Message, subscriber *Subscriber) bool
}

// An AllEventsFilter delivers all messages to a subscriber, provided that the
// subscriber is an administrator.
type AllEventsFilter struct {
	Filter
}

func (f *AllEventsFilter) String() string {
	return "all-events"
}

// Matches returns whether the current AllEventsFilter matches the provided
// subscriber.
func (f *AllEventsFilter) Matches(msg *Message, subscriber *Subscriber) bool {
	return subscriber.admin
}

// A UserFilter is a Filter that only allows Messages that are associated
// with a particular user.
type UserFilter struct {
	Filter
	user string
}

func (f *UserFilter) String() string {
	return fmt.Sprintf("user/%s", f.user)
}

// Matches returns whether the current UserFilter matches the provided
// message/subscriber combination.
func (f *UserFilter) Matches(msg *Message, subscriber *Subscriber) bool {
	return msg.User == subscriber.user
}

// A ProblemFilter is a Filter that only allows Messages that are associated
// with a particular problem.
type ProblemFilter struct {
	Filter
	problem string
}

func (f *ProblemFilter) String() string {
	return fmt.Sprintf("problem/%s", f.problem)
}

// Matches returns whether the current ProblemFilter matches the provided
// message/subscriber combination.
func (f *ProblemFilter) Matches(msg *Message, subscriber *Subscriber) bool {
	return msg.Problem == f.problem && (subscriber.admin || msg.Public ||
		(subscriber.user != "" && msg.User == subscriber.user) ||
		mapContains(subscriber.problemAdminMap, msg.Problem))
}

// A ContestFilter is a Filter that only allows Messages that are associated
// with a particular contest.
type ContestFilter struct {
	Filter
	contest string
	token   string
}

func (f *ContestFilter) String() string {
	if f.token != "" {
		return fmt.Sprintf("contest/%s/%s", f.contest, f.token)
	}
	return fmt.Sprintf("contest/%s", f.contest)
}

// Matches returns whether the current ContestFilter matches the provided
// message/subscriber combination.
func (f *ContestFilter) Matches(msg *Message, subscriber *Subscriber) bool {
	return msg.Contest == f.contest && (subscriber.admin || msg.Public ||
		(subscriber.user != "" && msg.User == subscriber.user) ||
		mapContains(subscriber.contestAdminMap, msg.Contest))
}

// NewFilter parses the provided filter stirng and constructs a new Filter
// instance.
func NewFilter(filter string) (Filter, error) {
	const errorString = "Invalid filter: %s"

	tokens := strings.Split(filter, "/")
	if len(tokens) < 2 {
		return nil, fmt.Errorf(errorString, filter)
	}
	if tokens[0] != "" {
		return nil, fmt.Errorf(errorString, filter)
	}
	switch tokens[1] {
	case "all-events":
		if len(tokens) == 2 {
			return &AllEventsFilter{}, nil
		}
	case "user":
		if len(tokens) == 3 {
			return &UserFilter{user: tokens[2]}, nil
		}
	case "problem":
		if len(tokens) == 3 {
			return &ProblemFilter{problem: tokens[2]}, nil
		}
	case "contest":
		switch len(tokens) {
		case 3:
			return &ContestFilter{contest: tokens[2]}, nil
		case 4:
			return &ContestFilter{contest: tokens[2], token: tokens[3]}, nil
		}
	}
	return nil, fmt.Errorf(errorString, filter)
}

func mapContains(m map[string]struct{}, k string) bool {
	_, ok := m[k]
	return ok
}
