package api

import (
	"github.com/hashicorp/nomad/api/contexts"
)

type Search struct {
	client *Client
}

// Search returns a handle on the Search endpoints
func (c *Client) Search() *Search {
	return &Search{client: c}
}

// PrefixSearch returns a set of matches for a particular context and prefix.
func (s *Search) PrefixSearch(prefix string, context contexts.Context, q *QueryOptions) (*SearchResponse, *QueryMeta, error) {
	var resp SearchResponse
	req := &SearchRequest{Prefix: prefix, Context: context}

	qm, err := s.client.putQuery("/v1/search", req, &resp, q)
	if err != nil {
		return nil, nil, err
	}

	return &resp, qm, nil
}

type SearchRequest struct {
	Prefix  string
	Context contexts.Context
	QueryOptions
}

// FuzzySearch returns a set of matches for a given context and string.
func (s *Search) FuzzySearch(text string, context []contexts.Context, q *QueryOptions) (*SearchResponse, *QueryMeta, error) {
	var resp SearchResponse

	c := make([]contexts.Context, len(context))
	copy(c, context)

	req := &FuzzySearchRequest{
		Text:     text,
		Contexts: c,
	}

	qm, err := s.client.putQuery("/v1/search/fuzzy", req, &resp, q)
	if err != nil {
		return nil, nil, err
	}

	return &resp, qm, nil
}

type FuzzySearchRequest struct {
	Text     string
	Contexts []contexts.Context
	QueryOptions
}

type SearchResponse struct {
	Matches     map[contexts.Context][]string
	Truncations map[contexts.Context]bool
	QueryMeta
}
