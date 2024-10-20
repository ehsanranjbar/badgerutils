// Code generated by mockery v2.46.3. DO NOT EDIT.

package mocks

import mock "github.com/stretchr/testify/mock"

// MockValueRetriever is an autogenerated mock type for the ValueRetriever type
type MockValueRetriever[T any] struct {
	mock.Mock
}

type MockValueRetriever_Expecter[T any] struct {
	mock *mock.Mock
}

func (_m *MockValueRetriever[T]) EXPECT() *MockValueRetriever_Expecter[T] {
	return &MockValueRetriever_Expecter[T]{mock: &_m.Mock}
}

// RetrieveValue provides a mock function with given fields: v
func (_m *MockValueRetriever[T]) RetrieveValue(v *T) ([]byte, error) {
	ret := _m.Called(v)

	if len(ret) == 0 {
		panic("no return value specified for RetrieveValue")
	}

	var r0 []byte
	var r1 error
	if rf, ok := ret.Get(0).(func(*T) ([]byte, error)); ok {
		return rf(v)
	}
	if rf, ok := ret.Get(0).(func(*T) []byte); ok {
		r0 = rf(v)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]byte)
		}
	}

	if rf, ok := ret.Get(1).(func(*T) error); ok {
		r1 = rf(v)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// MockValueRetriever_RetrieveValue_Call is a *mock.Call that shadows Run/Return methods with type explicit version for method 'RetrieveValue'
type MockValueRetriever_RetrieveValue_Call[T any] struct {
	*mock.Call
}

// RetrieveValue is a helper method to define mock.On call
//   - v *T
func (_e *MockValueRetriever_Expecter[T]) RetrieveValue(v interface{}) *MockValueRetriever_RetrieveValue_Call[T] {
	return &MockValueRetriever_RetrieveValue_Call[T]{Call: _e.mock.On("RetrieveValue", v)}
}

func (_c *MockValueRetriever_RetrieveValue_Call[T]) Run(run func(v *T)) *MockValueRetriever_RetrieveValue_Call[T] {
	_c.Call.Run(func(args mock.Arguments) {
		run(args[0].(*T))
	})
	return _c
}

func (_c *MockValueRetriever_RetrieveValue_Call[T]) Return(_a0 []byte, _a1 error) *MockValueRetriever_RetrieveValue_Call[T] {
	_c.Call.Return(_a0, _a1)
	return _c
}

func (_c *MockValueRetriever_RetrieveValue_Call[T]) RunAndReturn(run func(*T) ([]byte, error)) *MockValueRetriever_RetrieveValue_Call[T] {
	_c.Call.Return(run)
	return _c
}

// NewMockValueRetriever creates a new instance of MockValueRetriever. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
// The first argument is typically a *testing.T value.
func NewMockValueRetriever[T any](t interface {
	mock.TestingT
	Cleanup(func())
}) *MockValueRetriever[T] {
	mock := &MockValueRetriever[T]{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
