package broadcaster

import (
	"fmt"
	"strings"
)

type Filter interface {
	String() string
	Matches(msg *Message, subscriber *Subscriber) bool
}

type AllEventsFilter struct {
	Filter
}

func (f *AllEventsFilter) String() string {
	return "all-events"
}

func (f *AllEventsFilter) Matches(msg *Message, subscriber *Subscriber) bool {
	return subscriber.admin
}

type UserFilter struct {
	Filter
	user string
}

func (f *UserFilter) String() string {
	return fmt.Sprintf("user/%s", f.user)
}

func (f *UserFilter) Matches(msg *Message, subscriber *Subscriber) bool {
	return msg.User == subscriber.user
}

type ProblemFilter struct {
	Filter
	problem string
}

func (f *ProblemFilter) String() string {
	return fmt.Sprintf("problem/%s", f.problem)
}

func (f *ProblemFilter) Matches(msg *Message, subscriber *Subscriber) bool {
	return msg.Problem == f.problem && (subscriber.admin || msg.Public ||
		msg.User == subscriber.user || mapContains(subscriber.problemAdminMap,
		msg.Problem))
}

type ContestFilter struct {
	Filter
	contest string
	token   string
}

func (f *ContestFilter) String() string {
	if f.token != "" {
		return fmt.Sprintf("contest/%s/%s", f.contest, f.token)
	} else {
		return fmt.Sprintf("contest/%s", f.contest)
	}
}

func (f *ContestFilter) Matches(msg *Message, subscriber *Subscriber) bool {
	return msg.Contest == f.contest && (subscriber.admin || msg.Public ||
		msg.User == subscriber.user || mapContains(subscriber.contestAdminMap,
		msg.Contest))
}

func NewFilter(filter string) (Filter, error) {
	const kErrorString = "Invalid filter: %s"

	tokens := strings.Split(filter, "/")
	if len(tokens) < 2 {
		return nil, fmt.Errorf(kErrorString, filter)
	}
	if tokens[0] != "" {
		return nil, fmt.Errorf(kErrorString, filter)
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
	return nil, fmt.Errorf(kErrorString, filter)
}

func mapContains(m map[string]struct{}, k string) bool {
	_, ok := m[k]
	return ok
}
