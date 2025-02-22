package extutil_test

import (
	"testing"

	badger "github.com/dgraph-io/badger/v4"
	"github.com/ehsanranjbar/badgerutils/extutil"
	"github.com/ehsanranjbar/badgerutils/iters"
	estore "github.com/ehsanranjbar/badgerutils/store/entity"
	extstore "github.com/ehsanranjbar/badgerutils/store/ext"
	"github.com/ehsanranjbar/badgerutils/testutil"
	"github.com/stretchr/testify/suite"
)

type AssociateSuite struct {
	suite.Suite
	txn *badger.Txn
	ps  *estore.Store[int64, testutil.SampleEntity, *testutil.SampleEntity]
	cs  *estore.Store[int64, testutil.SampleEntity, *testutil.SampleEntity]
	rel *extutil.Association[int64, testutil.SampleEntity, *testutil.SampleEntity, int64, testutil.SampleEntity, *testutil.SampleEntity]
	psi *estore.Instance[int64, testutil.SampleEntity, *testutil.SampleEntity]
	csi *estore.Instance[int64, testutil.SampleEntity, *testutil.SampleEntity]
}

func TestAssociateSuite(t *testing.T) {
	suite.Run(t, new(AssociateSuite))
}

func (ts *AssociateSuite) SetupTest() {
	ts.txn = testutil.PrepareTxn(ts.T(), true)

	ts.ps = testutil.NewEntityStore([]byte("p"))
	ts.cs = testutil.NewEntityStore([]byte("c"))

	ts.rel = extutil.Associate("p-c-rel", ts.ps, ts.cs)

	ts.psi = ts.ps.Instantiate(ts.txn)
	ts.csi = ts.cs.Instantiate(ts.txn)
}

func (ts *AssociateSuite) TestParentStore() {
	p1 := testutil.NewSampleEntity("P1")
	p2 := testutil.NewSampleEntity("P2")
	c1 := testutil.NewSampleEntity("C1")
	c2 := testutil.NewSampleEntity("C2")

	err := ts.psi.Set(p1, extstore.WithExtOption(ts.rel.Name(), c1))
	ts.NoError(err)
	err = ts.psi.Set(p2, extstore.WithExtOption(ts.rel.Name(), c2))
	ts.NoError(err)

	dump := testutil.Dump(ts.txn)
	ts.Equal(
		map[string]string{
			"cd\x80\x00\x00\x00\x00\x00\x00\x01":                                        "{\"name\":\"C1\"}",
			"cd\x80\x00\x00\x00\x00\x00\x00\x02":                                        "{\"name\":\"C2\"}",
			"cxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01": "",
			"cxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x02": "",
			"pd\x80\x00\x00\x00\x00\x00\x00\x01":                                        "{\"name\":\"P1\"}",
			"pd\x80\x00\x00\x00\x00\x00\x00\x02":                                        "{\"name\":\"P2\"}",
			"pxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01": "",
			"pxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x02": "",
		},
		dump,
	)

	err = ts.psi.Delete(p1.Id)
	ts.NoError(err)

	dump = testutil.Dump(ts.txn)
	ts.Equal(
		map[string]string{
			"cd\x80\x00\x00\x00\x00\x00\x00\x02":                                        "{\"name\":\"C2\"}",
			"cxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x02": "",
			"pd\x80\x00\x00\x00\x00\x00\x00\x02":                                        "{\"name\":\"P2\"}",
			"pxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x02": "",
		},
		dump,
	)
}

func (ts *AssociateSuite) TestChildStore() {
	p1 := testutil.NewSampleEntity("P1")
	c1 := testutil.NewSampleEntity("C1")
	c2 := testutil.NewSampleEntity("C2")

	err := ts.csi.Set(c1)
	ts.Error(err)

	err = ts.csi.Set(c1, extstore.WithExtOption(ts.rel.Name(), int64(2)))
	ts.Error(err)

	err = ts.psi.Set(p1)
	ts.NoError(err)

	err = ts.csi.Set(c1, extstore.WithExtOption(ts.rel.Name(), p1.Id))
	ts.NoError(err)

	err = ts.csi.Set(c2, extstore.WithExtOption(ts.rel.Name(), p1.Id))
	ts.NoError(err)

	dump := testutil.Dump(ts.txn)
	ts.Equal(
		map[string]string{
			"cd\x80\x00\x00\x00\x00\x00\x00\x01":                                        "{\"name\":\"C1\"}",
			"cd\x80\x00\x00\x00\x00\x00\x00\x02":                                        "{\"name\":\"C2\"}",
			"cxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01": "",
			"cxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x01": "",
			"pd\x80\x00\x00\x00\x00\x00\x00\x01":                                        "{\"name\":\"P1\"}",
			"pxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01": "",
			"pxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x02": "",
		},
		dump,
	)

	err = ts.csi.Delete(c1.Id)
	ts.NoError(err)

	dump = testutil.Dump(ts.txn)
	ts.Equal(
		map[string]string{
			"cd\x80\x00\x00\x00\x00\x00\x00\x02":                                        "{\"name\":\"C2\"}",
			"cxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x02\x80\x00\x00\x00\x00\x00\x00\x01": "",
			"pd\x80\x00\x00\x00\x00\x00\x00\x01":                                        "{\"name\":\"P1\"}",
			"pxp-c-rel\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x02": "",
		},
		dump,
	)
}

func (ts *AssociateSuite) TestPIDFunc() {
	ps := testutil.NewEntityStore([]byte("g"))
	cs := testutil.NewEntityStore([]byte("f"))
	extutil.Associate("g-f-rel", ps, cs).WithPIDFunc(func(_ *testutil.SampleEntity) (int64, error) {
		return 1, nil
	})

	psi := ps.Instantiate(ts.txn)
	csi := cs.Instantiate(ts.txn)

	p1 := testutil.NewSampleEntity("P1")
	c1 := testutil.NewSampleEntity("C1")

	err := psi.Set(p1)
	ts.NoError(err)

	err = csi.Set(c1)
	ts.NoError(err)

	dump := testutil.Dump(ts.txn)
	ts.Equal(
		map[string]string{
			"fd\x80\x00\x00\x00\x00\x00\x00\x01":                                        "{\"name\":\"C1\"}",
			"gd\x80\x00\x00\x00\x00\x00\x00\x01":                                        "{\"name\":\"P1\"}",
			"gxg-f-rel\x80\x00\x00\x00\x00\x00\x00\x01\x80\x00\x00\x00\x00\x00\x00\x01": "",
		},
		dump,
	)

	err = csi.Delete(c1.Id)
	ts.NoError(err)

	dump = testutil.Dump(ts.txn)
	ts.Equal(
		map[string]string{
			"gd\x80\x00\x00\x00\x00\x00\x00\x01": "{\"name\":\"P1\"}",
		},
		dump,
	)
}

func (ts *AssociateSuite) TestAllowOrphans() {
	ps := testutil.NewEntityStore([]byte("g"))
	cs := testutil.NewEntityStore([]byte("f"))
	extutil.Associate("g-f-rel", ps, cs).AllowOrphans()

	c1 := testutil.NewSampleEntity("C1")

	csi := cs.Instantiate(ts.txn)

	err := csi.Set(c1)
	ts.NoError(err)
}

func (ts *AssociateSuite) TestInstance() {
	ps := testutil.NewEntityStore([]byte("g"))
	cs := testutil.NewEntityStore([]byte("f"))
	rel := extutil.Associate("g-f-rel", ps, cs).AllowOrphans()
	ins := rel.Instantiate(ts.txn)

	p1 := testutil.NewSampleEntity("P1")
	c1 := testutil.NewSampleEntity("C1")
	c2 := testutil.NewSampleEntity("C2")
	c3 := testutil.NewSampleEntity("C3")

	err := ins.Set(p1, c1, c2)
	ts.NoError(err)

	err = ins.Set(nil, c3)
	ts.NoError(err)

	p, err := ins.GetParent(c1.Id)
	ts.NoError(err)
	ts.Equal(p1, p)

	p, err = ins.GetParent(c3.Id)
	ts.NoError(err)
	ts.Nil(p)

	it, err := ins.GetChildrenIterator(p1.Id, badger.DefaultIteratorOptions)
	ts.NoError(err)
	defer it.Close()
	childs, err := iters.Collect(it)
	ts.NoError(err)

	ts.Equal([]*testutil.SampleEntity{c1, c2}, childs)
}
