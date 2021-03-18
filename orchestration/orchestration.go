package orchestration

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"sync"
)

type Status string

const (
	ExecuteStart      Status = "ExecuteStart"
	ExecuteSuccess    Status = "ExecuteSuccess"
	ExecuteFail       Status = "ExecuteFail"
	CompensateStart   Status = "CompensateStart"
	CompensateSuccess Status = "CompensateSuccess"
	CompensateFail    Status = "CompensateFail"
)

type Record struct {
}

type Storage interface {
	SaveStatus(ctx context.Context, id string, status Status) error
	List(ctx context.Context, status Status) ([]*Record, error)
}

type OrderSagaOrchestrator struct {
	paymentSagaMQ    MessageQueue
	partnerSagaMQ    MessageQueue
	restaurantSagaMQ MessageQueue

	sagaStatusStorage Storage
	stepStatusStorage Storage
}

func New(sagaStatusStorage, stepStatusStorage Storage) *OrderSagaOrchestrator {
	return &OrderSagaOrchestrator{sagaStatusStorage: sagaStatusStorage, stepStatusStorage: stepStatusStorage}
}

func (o *OrderSagaOrchestrator) Worker(ctx context.Context, sagaID string) error {
	runReceiversAsync := func(c context.Context, wg *sync.WaitGroup, mq MessageQueue) {
		go func() {
			wg.Add(1)
			mq.RunReceiver(c)
			wg.Done()
		}()
	}

	g := &sync.WaitGroup{}
	runReceiversAsync(ctx, g, o.paymentSagaMQ)
	runReceiversAsync(ctx, g, o.partnerSagaMQ)
	runReceiversAsync(ctx, g, o.restaurantSagaMQ)
	g.Wait()

	return nil
}

func (o *OrderSagaOrchestrator) CreateSaga(ctx context.Context, sagaID string) error {
	if err := o.sagaStatusStorage.SaveStatus(ctx, sagaID, ExecuteStart); err != nil {
		return err
	}

	return nil
}

func NewOrchestrator(paymentSagaMQ, partnerSagaMQ, restaurantSagaMQ MessageQueue) *OrderSagaOrchestrator {
	return &OrderSagaOrchestrator{
		paymentSagaMQ:    paymentSagaMQ,
		partnerSagaMQ:    partnerSagaMQ,
		restaurantSagaMQ: restaurantSagaMQ,
	}
}

type HTTPServer struct {
	orderSagaOrchestrator *OrderSagaOrchestrator
}

func NewHTTPServer(o *OrderSagaOrchestrator) *HTTPServer {
	return &HTTPServer{orderSagaOrchestrator: o}
}

type Param struct {
	UserID       string `json:"user_id"`
	RestaurantID string `json:"restaurant_id"`
	FoodID       string `json:"food_id"`
}

func (s *HTTPServer) orderHandler() func(w http.ResponseWriter, r *http.Request) {
	return func(w http.ResponseWriter, r *http.Request) {
		var param Param
		if err := json.NewDecoder(r.Body).Decode(&param); err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		if err := s.orderSagaOrchestrator.CreateSaga(r.Context(), param.UserID); err != nil {
			log.Printf("failed to create saga: %s\n", err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		w.WriteHeader(http.StatusCreated)
	}
}

func main() {
	// storage := NewMySQLStorage()
	orderSagaOrchestrator := New(nil)
	s := NewHTTPServer(orderSagaOrchestrator)
	http.HandleFunc("/order", s.orderHandler())
	http.ListenAndServe(":8080", nil)
}

type DeliveryPartnerService struct{}

type RestaurantService struct{}

type PaymentService struct{}

func NewDeliveryPartnerService() *DeliveryPartnerService {
	return &DeliveryPartnerService{}
}

func NewRestaurantService() *RestaurantService {
	return &RestaurantService{}
}

func NewPaymentService() *PaymentService {
	return &PaymentService{}
}

func (d *DeliveryPartnerService) EnsurePartner(ctx context.Context) error {
	// Invoke RPC to delivery partner microservice
	// It might succeed, fail, succeeded but response not returned
	return nil
}
