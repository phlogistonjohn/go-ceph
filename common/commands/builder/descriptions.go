package builder

import (
	"encoding/json"
	"maps"
	"slices"
	"strings"
)

type SignatureVar struct {
	Name    string `json:"name"`
	Type    string `json:"type"`
	Req     bool   `json:"req"`
	Choices string `json:"strings"`
	Repeat  string `json:"n"`
}

type SignatureElement struct {
	Static   string
	Variable *SignatureVar
}

func (se *SignatureElement) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &se.Static); err == nil {
		return nil
	}
	se.Variable = new(SignatureVar)
	return json.Unmarshal(data, &se.Variable)
}

type Description struct {
	Key    string
	Sig    []*SignatureElement `json:"sig"`
	Help   string              `json:"help"`
	Module string              `json:"module"`
	Perm   string              `json:"perm"`
	Flags  uint64              `json:"flags"`
}

func (d Description) Prefix() []string {
	p := []string{}
	for _, elem := range d.Sig {
		if elem.Variable != nil {
			break
		}
		p = append(p, elem.Static)
	}
	return p
}

func (d Description) PrefixString() string {
	return strings.Join(d.Prefix(), " ")
}

func (d Description) Variables() []*SignatureVar {
	v := []*SignatureVar{}
	for _, elem := range d.Sig {
		if elem.Variable == nil {
			continue
		}
		v = append(v, elem.Variable)
	}
	return v
}

type CommandDescriptions struct {
	Entries []Description
}

func (cd *CommandDescriptions) UnmarshalJSON(data []byte) error {
	cdmap := map[string]json.RawMessage{}
	err := json.Unmarshal(data, &cdmap)
	if err != nil {
		return err
	}
	for _, key := range slices.Sorted(maps.Keys(cdmap)) {
		var desc Description
		err = json.Unmarshal(cdmap[key], &desc)
		if err != nil {
			return err
		}
		desc.Key = key
		cd.Entries = append(cd.Entries, desc)
	}
	return nil
}

func matchDescriptions(ds []Description, index int, term string) []Description {
	out := []Description{}
	for _, desc := range ds {
		s := desc.Sig[index]
		if s.Variable == nil && s.Static == term {
			out = append(out, desc)
		}
	}
	return out
}

func (cd *CommandDescriptions) Match(terms []string) []Description {
	matches := cd.Entries
	for idx, term := range terms {
		matches = matchDescriptions(matches, idx, term)
	}
	return matches
}

func (cd *CommandDescriptions) Find(n ...string) []Description {
	return cd.Match(n)
}
