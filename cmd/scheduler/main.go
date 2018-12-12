package main

import (
	"fmt"
	"log"
	"math/rand"
	"time"

	"k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/informers"
	coreinformers "k8s.io/client-go/informers/core/v1"
	"k8s.io/client-go/kubernetes"
	listersv1 "k8s.io/client-go/listers/core/v1"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/cache"
)

const schedulerName = "random-scheduler"

type Scheduler struct {
	clientset  *kubernetes.Clientset
	nodeLister listersv1.NodeLister
}

func NewScheduler(stopCh chan struct{}) Scheduler {
	config, err := rest.InClusterConfig()
	if err != nil {
		log.Fatal(err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		log.Fatal(err)
	}

	return Scheduler{
		clientset:  clientset,
		nodeLister: initInformers(clientset, stopCh),
	}
}

func initInformers(clientset *kubernetes.Clientset, stopCh chan struct{}) listersv1.NodeLister {
	factory := informers.NewSharedInformerFactory(clientset, 0)

	var nodeInformer coreinformers.NodeInformer
	nodeInformer = factory.Core().V1().Nodes()

	nodeInformer.Informer().AddEventHandler(cache.ResourceEventHandlerFuncs{
		AddFunc: func(obj interface{}) {
			node, ok := obj.(*v1.Node)
			if !ok {
				log.Println("this is not a node")
				return
			}
			log.Printf("New Node Added to Store: %s", node.GetName())
		},
	})

	factory.Start(stopCh)
	return nodeInformer.Lister()
}

func main() {
	fmt.Println("I'm a scheduler!")

	rand.Seed(time.Now().Unix())

	stopCh := make(chan struct{})
	defer close(stopCh)

	scheduler := NewScheduler(stopCh)
	scheduler.SchedulePods()
}

func (s *Scheduler) SchedulePods() error {

	watch, err := s.clientset.CoreV1().Pods("").Watch(metav1.ListOptions{
		FieldSelector: fmt.Sprintf("spec.schedulerName=%s,spec.nodeName=", schedulerName),
	})
	if err != nil {
		log.Println("error when watching pods", err.Error())
		return err
	}

	for event := range watch.ResultChan() {
		if event.Type != "ADDED" {
			continue
		}
		p, ok := event.Object.(*v1.Pod)
		if !ok {
			fmt.Println("unexpected type")
			continue
		}

		fmt.Println("found a pod to schedule:", p.Namespace, "/", p.Name)

		node, err := s.findFit()
		if err != nil {
			log.Println("cannot find node that fits pod", err.Error())
			continue
		}

		err = s.bindPod(p, node)
		if err != nil {
			log.Println("failed to bind pod", err.Error())
			continue
		}

		message := fmt.Sprintf("Placed pod [%s/%s] on %s\n", p.Namespace, p.Name, node.Name)

		err = s.emitEvent(p, message)
		if err != nil {
			log.Println("failed to emit scheduled event", err.Error())
			continue
		}

		fmt.Println(message)
	}
	return nil
}

func (s *Scheduler) findFit() (*v1.Node, error) {
	nodes, err := s.nodeLister.List(labels.Everything())
	if err != nil {
		return nil, err
	}
	return nodes[rand.Intn(len(nodes))], nil
}

func (s *Scheduler) bindPod(p *v1.Pod, randomNode *v1.Node) error {
	return s.clientset.CoreV1().Pods(p.Namespace).Bind(&v1.Binding{
		ObjectMeta: metav1.ObjectMeta{
			Name:      p.Name,
			Namespace: p.Namespace,
		},
		Target: v1.ObjectReference{
			APIVersion: "v1",
			Kind:       "Node",
			Name:       randomNode.Name,
		},
	})
}

func (s *Scheduler) emitEvent(p *v1.Pod, message string) error {
	timestamp := time.Now().UTC()
	_, err := s.clientset.CoreV1().Events(p.Namespace).Create(&v1.Event{
		Count:          1,
		Message:        message,
		Reason:         "Scheduled",
		LastTimestamp:  metav1.NewTime(timestamp),
		FirstTimestamp: metav1.NewTime(timestamp),
		Type:           "Normal",
		Source: v1.EventSource{
			Component: schedulerName,
		},
		InvolvedObject: v1.ObjectReference{
			Kind:      "Pod",
			Name:      p.Name,
			Namespace: p.Namespace,
			UID:       p.UID,
		},
		ObjectMeta: metav1.ObjectMeta{
			GenerateName: p.Name + "-",
		},
	})
	if err != nil {
		return err
	}
	return nil
}
