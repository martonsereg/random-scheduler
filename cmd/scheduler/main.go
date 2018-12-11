package main

import (
	"fmt"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/api/core/v1"
	"log"
	"time"
	"math/rand"
)

const schedulerName = "random-scheduler"

func main() {
	fmt.Println("I'm a scheduler!")

	rand.Seed(time.Now().Unix())

	config, err := rest.InClusterConfig()
	if err != nil {
		fmt.Println(err)
		return
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		fmt.Println(err)
		return
	}

	err = schedulePods(clientset)

}

func schedulePods(clientset *kubernetes.Clientset) error {
	watch, err := clientset.CoreV1().Pods("").Watch(metav1.ListOptions{
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

		node, err := findFit(clientset)
		if err != nil {
			log.Println("cannot find node that fits pod", err.Error())
			continue
		}

		err = bindPod(clientset, p, node)
		if err != nil {
			log.Println("failed to bind pod", err.Error())
			continue
		}

		message := fmt.Sprintf("Placed pod [%s/%s] on %s\n", p.Namespace, p.Name, node.Name)

		err = emitEvent(clientset, p, message)
		if err != nil {
			log.Println("failed to emit scheduled event", err.Error())
			continue
		}

		fmt.Println(message)
	}
	return nil
}

func findFit(clientset *kubernetes.Clientset) (*v1.Node, error) {
	nodes, err := clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		return nil, err
	}
	return &nodes.Items[rand.Intn(len(nodes.Items))], nil
}

func bindPod(clientset *kubernetes.Clientset, p *v1.Pod, randomNode *v1.Node) error {
	return clientset.CoreV1().Pods(p.Namespace).Bind(&v1.Binding{
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

func emitEvent(clientset *kubernetes.Clientset, p *v1.Pod, message string) error {
	timestamp := time.Now().UTC()
	_, err := clientset.CoreV1().Events(p.Namespace).Create(&v1.Event{
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
