#include <algorithm>
#include <chrono>
#include <cmath>
#include <iostream>
#include <memory>
#include <string>
#include <fstream>
#include <thread>
#include <sys/time.h>
#include <unistd.h>
#include <mpi.h>
#include <getopt.h>
#include <sstream>

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include "benchmark.grpc.pb.h"

using grpc::Channel;
using grpc::ClientAsyncResponseReader;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using grpc::CompletionQueue;
using grpc::Status;
using benchmark::BENCHMARK;
using benchmark::Chunk;

int mathToInt(std::string math) {
    std::stringstream mathStrm(math);
    int result;
    mathStrm >> result;
    char op;
    int num;
    while(mathStrm >> op >> num) result *= num;
    return result;
}

class ClientNode {
    public:
        ClientNode(std::shared_ptr<Channel> channel){
            stub_ = BENCHMARK::NewStub(channel);
        }

        ~ClientNode() {
            deleteBuffer();
        }

        void createBuffer(size_t chunk_size) {
            deleteBuffer();
            chunk_size_ = chunk_size;
            chunk_data_ = new char[chunk_size];
            for (size_t i = 0; i < chunk_size; i++) chunk_data_[i] = 'a' + (i % 26);
            chunk_.set_data(chunk_data_, chunk_size);
        }

        void deleteBuffer() {
            if(chunk_size_ != 0) delete[] chunk_data_;
        }

        size_t getChunkSize() {
            return chunk_.data().length();
        }

        bool helloThere() {
            Chunk request, reply;
            ClientContext context;
            CompletionQueue cq;
            Status status;

            request.set_data("hello", 5);

            std::unique_ptr<ClientAsyncResponseReader<Chunk> > rpc(
                stub_->PrepareAsynchelloThere(&context, request, &cq));

            rpc->StartCall();
            
            rpc->Finish(&reply, &status, (void*)1);

            void* got_tag;
            bool ok = false;

            GPR_ASSERT(cq.Next(&got_tag, &ok));

            GPR_ASSERT(got_tag == (void*)1);

            GPR_ASSERT(ok);

            // printf("helloThere successful: %d\n", status.ok());
            if (!status.ok()) printf("helloThere failed\n");

            return status.ok();
        }

        double latencyTest(size_t chunk_num) {
            double timeuse1, timeSum = 0.0;
            struct timeval t0, t1;
            for (size_t i = 0; i < chunk_num; i++) {
                gettimeofday(&t0, NULL);

                Chunk reply;
                ClientContext context;
                CompletionQueue cq;
                Status status;

                std::unique_ptr<ClientAsyncResponseReader<Chunk> > rpc(
                    stub_->PrepareAsynclatencyTest(&context, chunk_, &cq));

                rpc->StartCall();

                rpc->Finish(&reply, &status, (void*)1);

                void* got_tag;
                bool ok = false;

                GPR_ASSERT(cq.Next(&got_tag, &ok));

                GPR_ASSERT(got_tag == (void*)1);

                GPR_ASSERT(ok);

                if(!status.ok()) {
                    printf("Client: rpc failed, try again\n");
                    return latencyTest(chunk_num);
                }

                gettimeofday(&t1, NULL);
                timeuse1 = (t1.tv_sec - t0.tv_sec) + (double)(t1.tv_usec - t0.tv_usec)/1000000.0;
                timeSum += timeuse1;
            }

            double latency = timeSum / chunk_num;
            return latency;
        }

        double throughputTest(size_t chunk_num) {
            struct timeval t0, t1;
  	        double timeuse1;
  	        gettimeofday(&t0, NULL);
            for (size_t i = 0; i < chunk_num; i++) {
                Chunk reply;
                ClientContext context;
                CompletionQueue cq;
                Status status;

                std::unique_ptr<ClientAsyncResponseReader<Chunk> > rpc(
                    stub_->PrepareAsyncthroughputTest(&context, chunk_, &cq));
                
                rpc->StartCall();

                rpc->Finish(&reply, &status, (void*)1);

                void* got_tag;
                bool ok = false;

                GPR_ASSERT(cq.Next(&got_tag, &ok));

                GPR_ASSERT(got_tag == (void*)1);

                GPR_ASSERT(ok);

                if(!status.ok()) {
                    printf("Client: rpc failed, try again\n");
                    return latencyTest(chunk_num);
                }
            }

            gettimeofday(&t1, NULL);
            timeuse1 = (t1.tv_sec - t0.tv_sec) + (double)(t1.tv_usec - t0.tv_usec)/1000000.0;
            double total_chunk_size = chunk_num * chunk_size_;
            double throughput = total_chunk_size / timeuse1;
            return throughput;

        }

private:
		std::unique_ptr<BENCHMARK::Stub> stub_;
		std::shared_ptr<Channel> channel_;
        char *chunk_data_;
        size_t chunk_size_ = 0;
        Chunk chunk_;
};

void ThreadRunThis(ClientNode* client, size_t chunk_num, size_t chunk_size1, size_t chunk_size2, 
        int world_rank, int world_size) {
    for(size_t chunk_size = chunk_size1; chunk_size <= chunk_size2; chunk_size *= 2){
        client->createBuffer(chunk_size);
        double latency = client->latencyTest(chunk_num);
        double throughput = client->throughputTest(chunk_num);
        if (world_rank != 0){
            MPI_Send(&latency, 1, MPI_DOUBLE, 0, 0, MPI_COMM_WORLD);
            MPI_Send(&throughput, 1, MPI_DOUBLE, 0, 1, MPI_COMM_WORLD);
        } else {
            double latency_avg = latency;
            double throughput_total = throughput;
            double* latencies;
            latencies = new double[world_size];
            latencies[0] = latency;
            for (int i = 1; i < world_size; i++) {
                double latency_, throughput_;
                MPI_Recv(&latency_, 1, MPI_DOUBLE, i, 0, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                MPI_Recv(&throughput_, 1, MPI_DOUBLE, i, 1, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
                latency_avg += latency_;
                throughput_total += throughput_;
                latencies[i] = latency_;
            }
            latency_avg /= world_size;
            sort(latencies, latencies + world_size, std::greater<double>());
            double latency_median = latencies[world_size / 2];
            printf("chunk_num = %d, chunk_size = %9d(B), latency avg = %10.4f(ms), latency median = %10.4f(ms), total throughput = %10.4f(MB/s)\n",
                    chunk_num, client->getChunkSize(), latency_avg * 1000, latency_median * 1000, throughput_total / 1024 / 1024);
        }
    }
}

int main(int argc, char** argv) {

    MPI_Init(&argc, &argv);

    int world_size;
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);

    int world_rank;
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);

    if(world_rank == 0) printf("world size = %d\n", world_size);

    size_t chunk_size1;
    size_t chunk_size2;
    size_t chunk_num;
    std::string server_addr;

    int flag;
    while((flag = getopt(argc, argv, "s:a:b:n:")) != -1) {
        switch (flag) {
            case 's':
                server_addr = std::string(optarg);
                break;
            case 'n':
                chunk_num = atoi(optarg);
                break;
            case 'a':
                chunk_size2 = chunk_size1 = mathToInt(std::string(optarg));
                break;
            case 'b':
                chunk_size2 = mathToInt(std::string(optarg));
                break;
            default:
                exit(EXIT_FAILURE);
        }
    }

    ClientNode* client = new  ClientNode(grpc::CreateChannel(server_addr, grpc::InsecureChannelCredentials()));
    client->helloThere();
    ThreadRunThis(client, chunk_num, chunk_size1, chunk_size2, world_rank, world_size);

    MPI_Finalize();
    
}