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

#include <grpc/grpc.h>
#include <grpcpp/channel.h>
#include <grpcpp/client_context.h>
#include <grpcpp/create_channel.h>
#include <grpcpp/security/credentials.h>
#include "benchmark.grpc.pb.h"

using grpc::Channel;
using grpc::ClientContext;
using grpc::ClientReader;
using grpc::ClientReaderWriter;
using grpc::ClientWriter;
using grpc::Status;
using benchmark::BENCHMARK;
using benchmark::Chunk;

class ClientNode {
    public:
        ClientNode(std::string server_address) {
			channel_ = grpc::CreateChannel(server_address, grpc::InsecureChannelCredentials());
			stub_ = BENCHMARK::NewStub(channel_);
            chunk_size_ = 0;
		}

        ~ClientNode() {
            deleteBuffer();
        }

        size_t getChunkSize() {
            return chunk_.data().length();
        }

        void deleteBuffer() {
            if(chunk_size_ != 0) delete[] chunk_data_;
        }

        void createBuffer(size_t chunk_size) {
            deleteBuffer();
            chunk_size_ = chunk_size;
            chunk_data_ = new char[chunk_size];
            for (size_t i = 0; i < chunk_size; i++) chunk_data_[i] = 'a' + (i % 26);
            chunk_.set_data(chunk_data_, chunk_size);
        }

        bool helloThere() {
            Chunk chunk, reply;
            ClientContext context;
            Status status = stub_->helloThere(&context, chunk, &reply);
            return status.ok();
        }

        double latencyTest(size_t chunk_num) {
            double timeuse1, timeSum = 0.0;
            struct timeval t0, t1;
            for (size_t i = 0; i < chunk_num; i++) {
  	            gettimeofday(&t0, NULL);

                Chunk reply;
                ClientContext context;
                Status status = stub_->latencyTest(&context, chunk_, &reply);
                if(!status.ok()) {
                    printf("Client: rpc failed, try again\n");
                    sleep(1);
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

            for(size_t i = chunk_num; i > 0; i--) {
                Chunk reply;
                ClientContext context;
                Status status = stub_->throughputTest(&context, chunk_, &reply);
                if(!status.ok()) {
                    printf("Client: rpc failed, try again\n");
                    sleep(1);
                    return throughputTest(chunk_num);
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

int main (int argc, char** argv)
{
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
                chunk_size1 = atoi(optarg);
                break;
            case 'b':
                chunk_size2 = atoi(optarg);
                break;
            default:
                exit(EXIT_FAILURE);
        }
    } 

    {
        ClientNode client(server_addr);
        if(!client.helloThere()) return 0;

        for(size_t chunk_size = chunk_size1; chunk_size <= chunk_size2; chunk_size *= 2){
            client.createBuffer(chunk_size);
            double latency = client.latencyTest(chunk_num);
            double throughput = client.throughputTest(chunk_num);

            printf("chunk_num = %d, chunk_size = %9d(bytes), latency = %10.5f(ms), throughput = %10.5f(MB/s)\n",
                chunk_num, client.getChunkSize(), latency * 1000, throughput / 1024 / 1024);

            // sleep(1);

            if(chunk_num > 5) chunk_num--;
        }
    }

	return 0;
}