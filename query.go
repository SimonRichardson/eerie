package eerie

import (
	"github.com/SimonRichardson/wishful/useful"
	"github.com/SimonRichardson/wishful/wishful"
	"labix.org/v2/mgo"
	"labix.org/v2/mgo/bson"
)

type Query interface {
	CollectionName() string
}

type FindQuery struct {
	Name     string
	Selector bson.M
	Value    wishful.AnyVal
}

func (q FindQuery) CollectionName() string {
	return q.Name
}

type InsertQuery struct {
	Name  string
	Value wishful.AnyVal
}

func (q InsertQuery) CollectionName() string {
	return q.Name
}

type UpdateQuery struct {
	Name     string
	Selector bson.M
	Change   bson.M
}

func (q UpdateQuery) CollectionName() string {
	return q.Name
}

type UpsertQuery struct {
	Name     string
	Selector bson.M
	Value    wishful.AnyVal
}

func (q UpsertQuery) CollectionName() string {
	return q.Name
}

type Queue interface {
	Add(query Query) Queue
	GetAll() []Query
}

type SequentialQueue struct {
	queries []Query
}

func (q SequentialQueries) Add(query Query) Queue {
	return SequentialQueue{
		queries: append(q.queries, query),
	}
}

func (q SequentialQueries) GetAll() []Query {
	return q.queries
}

type Executioner interface {
	Execute(queue Queue)
}

type SequentialExecutioner struct {
	name    string
	session *mgo.Session
}

func (e SequentialExecutioner) Execute(queue Queue) ([]*mgo.Query, error) {
    results := []*mgo.Query

	db := e.session.DB(e.name)
	for _, q := range queue.GetAll() {
		col := db.C(q.CollectionName())
		if p, ok := q.(FindQuery); ok {
			results = append(results, col.Find(p.Selector))
		} else if p, ok := q.(InsertQuery); ok {
            col.Insert(p.Value)
        }
	}

    return results
}
