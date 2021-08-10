package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"time"

	"github.com/rafaelpapastamatiou/go-grpc/pb"
	"google.golang.org/grpc"
)

func main() {
	connection, err := grpc.Dial("localhost:50051", grpc.WithInsecure())

	if err != nil {
		log.Fatalf("Could not connect to gRPC Server: %v", err)
	}

	defer connection.Close()

	client := pb.NewUserServiceClient(connection)

	AddUserStreamBoth(client)
}

func AddUser(client pb.UserServiceClient) {
	req := &pb.User{
		Id:    "0",
		Name:  "Rafael",
		Email: "rafaelpapastamatiou@gmail.com",
	}

	res, err := client.AddUser(context.Background(), req)

	if err != nil {
		log.Fatalf("Could not make gRPC request: %v", err)
	}

	fmt.Println(res)
}

func AddUserVerbose(client pb.UserServiceClient) {
	req := &pb.User{
		Id:    "0",
		Name:  "Rafael",
		Email: "rafaelpapastamatiou@gmail.com",
	}

	responseStream, err := client.AddUserVerbose(context.Background(), req)

	if err != nil {
		log.Fatalf("Could not make gRPC request: %v", err)
	}

	for {
		stream, err := responseStream.Recv()

		if err == io.EOF {
			break
		}

		if err != nil {
			log.Fatalf("Could not receive the message: %v", err)
		}

		fmt.Println("Status: ", stream.Status)
	}

}

func AddUsers(client pb.UserServiceClient) {
	reqs := []*pb.User{
		&pb.User{
			Id:    "1",
			Name:  "Rafael",
			Email: "rafaelpapastamatiou@gmail.com",
		},
		&pb.User{
			Id:    "2",
			Name:  "Rafael 2",
			Email: "rafaelpapastamatiou2@gmail.com",
		},
		&pb.User{
			Id:    "3",
			Name:  "Rafael 3",
			Email: "rafaelpapastamatiou3@gmail.com",
		},
		&pb.User{
			Id:    "4",
			Name:  "Rafael 4",
			Email: "rafaelpapastamatiou4@gmail.com",
		},
		&pb.User{
			Id:    "5",
			Name:  "Rafael 5",
			Email: "rafaelpapastamatiou5@gmail.com",
		},
	}

	stream, err := client.AddUsers(context.Background())

	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	for _, req := range reqs {
		stream.Send(req)
		time.Sleep(time.Second * 2)
	}

	res, err := stream.CloseAndRecv()

	if err != nil {
		log.Fatalf("Error receiving response: %v", err)
	}

	fmt.Println(res)
}

func AddUserStreamBoth(client pb.UserServiceClient) {
	stream, err := client.AddUserStreamBoth(context.Background())

	if err != nil {
		log.Fatalf("Error creating request: %v", err)
	}

	reqs := []*pb.User{
		&pb.User{
			Id:    "1",
			Name:  "Rafael",
			Email: "rafaelpapastamatiou@gmail.com",
		},
		&pb.User{
			Id:    "2",
			Name:  "Rafael 2",
			Email: "rafaelpapastamatiou2@gmail.com",
		},
		&pb.User{
			Id:    "3",
			Name:  "Rafael 3",
			Email: "rafaelpapastamatiou3@gmail.com",
		},
		&pb.User{
			Id:    "4",
			Name:  "Rafael 4",
			Email: "rafaelpapastamatiou4@gmail.com",
		},
		&pb.User{
			Id:    "5",
			Name:  "Rafael 5",
			Email: "rafaelpapastamatiou5@gmail.com",
		},
	}

	wait := make(chan int)

	go func() {
		for _, req := range reqs {
			fmt.Println("Sending user: ", req.Name)
			stream.Send(req)
			time.Sleep(time.Second * 2)
		}

		stream.CloseSend()
	}()

	go func() {
		for {
			res, err := stream.Recv()

			if err == io.EOF {
				break
			}

			if err != nil {
				log.Fatalf("Error receiving data from the server: %v", err)
				break
			}

			fmt.Printf(
				"Receiving user %v com status %v\n",
				res.GetUser().GetName(),
				res.GetStatus(),
			)
		}

		close(wait)
	}()

	<-wait
}
