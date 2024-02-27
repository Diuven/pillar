#include <atomic>
#include <iostream>
#include <utility>
#include <random>
#include <set>
#include <cassert>
#include <functional>
#include <thread>
#include <vector>
#include <chrono>

struct Node;

struct NdPtr
{
    Node *ptr;
    bool marked; // change to lower bit?
    NdPtr(Node *p = nullptr, bool m = false) : ptr(p), marked(m) {}
};

struct Node
{
    int key;
    std::atomic<NdPtr> next;
    Node(int k = 0) : key(k) {}
    // print key, ptr, mark for debugging
    void print()
    {
        std::cout << "Node : " << key << " " << next.load().ptr << " " << next.load().marked << std::endl;
    }
};

typedef std::pair<Node *, Node *> NodePair;

// Questions:
// atomic guarantees that cas, read and write is consistent? no need to make it volatile?
// is mark also included as a part of CAS? is it compared properly?
// what if delete and insert contends with same key? does marking work ok?
// should I deliberately free the nodes? (I should, right?)
// ref-count list?
// lock-free vs deadlock-free?
// this is not wait-free, but it's lock-free, right? i.e. each operations are not bounded.

struct HarrisList
{
    Node *head, *tail;

    HarrisList()
    {
        head = new Node();
        tail = new Node();
        head->next.store(NdPtr(tail, false));
    }

public:
    bool insert(int key)
    {
        Node *new_node = new Node(key);
        Node *right_node, *left_node;

        do
        {
            NodePair nodes = search(key);
            left_node = nodes.first;
            right_node = nodes.second;
            NdPtr right_node_ptr = NdPtr(right_node, false);

            // Already have it
            if ((right_node != tail) && (right_node->key == key)) // T1
            {
                return false;
            }

            // Prepare new node
            new_node->next.store(right_node_ptr);

            // Swap the new node in
            // if right node is changed, or marked to be deleted, restart the process
            bool same_state = left_node->next.compare_exchange_strong(
                right_node_ptr, (NdPtr(new_node, false)));
            if (same_state) // C2
            {
                return true;
            }
        } while (true); // B3
    }

    bool erase(int key)
    {
        Node *right_node, *left_node;
        NdPtr right_node_next;

        do
        {
            NodePair nodes = search(key);
            left_node = nodes.first;
            right_node = nodes.second; // target node to erase

            // Not found
            if ((right_node == tail) || (right_node->key != key)) // T1
            {
                return false;
            }

            // Try to mark the node
            right_node_next = right_node->next.load();
            if (!right_node_next.marked)
            {
                bool same_state = right_node->next.compare_exchange_strong(
                    right_node_next, NdPtr(right_node_next.ptr, true)); // C3
                if (same_state)
                {
                    break;
                }
            }
        } while (true); // B4

        // Remove node from list
        NdPtr right_node_ptr = NdPtr(right_node, false);
        bool did_erase = left_node->next.compare_exchange_strong(
            right_node_ptr, right_node_next); // C4
        if (!did_erase)
        {
            search(key);
        }
        return true;
    }

    bool find(int key)
    {
        Node *right_node = search(key).second;
        return (right_node != tail) && (right_node->key == key);
    }

    NodePair search(int search_key)
    {
        Node *left_node, *left_node_next, *right_node;

        do
        {
            Node *t = head;
            NdPtr t_next = head->next.load();

            // Find left_node and right_node
            do
            {
                if (!t_next.marked)
                {
                    left_node = t;
                    left_node_next = t_next.ptr;
                }
                t = t_next.ptr;
                if (t == tail)
                {
                    break;
                }
                t_next = t->next.load();

            } while (t_next.marked || t->key < search_key); // B1
            right_node = t;

            // Check if nodes are adjacent
            if (left_node_next == right_node)
            {
                if ((right_node != tail) && right_node->next.load().marked)
                {
                    continue; // G1
                }
                return NodePair(left_node, right_node); // R1
            }

            // Remove one or more marked nodes in between
            NdPtr _tmp_left_next = NdPtr(left_node_next, false);
            bool same_state = left_node->next.compare_exchange_strong(
                _tmp_left_next, NdPtr(right_node, false)); // C1
            if (same_state)
            {
                if ((right_node != tail) && right_node->next.load().marked)
                {
                    continue; // G2
                }
                return NodePair(left_node, right_node); // R2
            }
        } while (true); // B2
    }

    // TODO destructor

    void print() // not thread-safe
    {
        int size = 0;
        std::cout << "--- Printing list: " << std::endl;
        Node *t = head;
        while (t != tail)
        {
            t->print();
            size += 1;
            t = t->next.load().ptr;
        }
        std::cout << "--- Size: " << size << std::endl;
        std::cout << "--- End of list" << std::endl;
    }

    int size() // not thread-safe
    {
        int size = 0;
        Node *t = head;
        while (t != tail)
        {
            size += 1;
            t = t->next.load().ptr;
        }
        return size;
    }
};

class LinearCongruentialGenerator
{
private:
    unsigned long a;
    unsigned long b;
    unsigned long m;
    unsigned long current;

public:
    LinearCongruentialGenerator()
        : a(0), b(0), m(0), current(0) {}

    LinearCongruentialGenerator(int a, int b, int m, int seed)
        : a(a), b(b), m(m), current(seed) {}

    int next()
    {
        current = (a * current + b) % m;
        return current;
    }
};

const int A = 48271, B = 3885;
const int MAX = 5e8;

typedef std::pair<int, int> Operation;
class OperationGenerator
{
private:
    int seed;
    int mn;
    int mx;
    int iratio;
    LinearCongruentialGenerator gen;

public:
    OperationGenerator(int seed, int mn, int mx, int iratio)
    {
        this->seed = seed;
        this->mn = mn;
        this->mx = mx;
        this->iratio = iratio;
        gen = LinearCongruentialGenerator(A, B, mx - mn, seed);
    }

    Operation next()
    {
        int op = gen.next() % 100;
        int key = gen.next() % (mx - mn) + mn;
        if (op < iratio)
        {
            return Operation(0, key);
        }
        else
        {
            return Operation(1, key);
        }
    }
};

void multi_test(int thread_count)
{
    HarrisList list;

    // const int INIT_SIZE = 50;
    const int INIT_SIZE = 2e4;
    const int TOTAL_SIZE = 5e5;

    // std::set<int> keys;
    std::vector<Operation> ops;
    std::set<Operation> op_set;

    int time_seed = std::chrono::system_clock::now().time_since_epoch().count() % 1000000;
    std::cout << "Seed: " << time_seed << std::endl;
    auto mt_gen = std::mt19937(time_seed);
    std::uniform_int_distribution<int> distribution(1, MAX);

    // fill list with initial elements and test
    {
        std::cout << " --- Initial test --- " << std::endl;
        auto seed = time_seed * 1000 + (-1);

        std::cout << "Inserting initial elements" << std::endl;
        auto gen = OperationGenerator(seed, 10, MAX, 100);
        for (int i = 0; i < INIT_SIZE; i++)
        {
            auto op = gen.next();
            assert(op.first == 0);
            list.insert(op.second);
            ops.push_back(op);
            op_set.insert(op);
        }

        std::cout << "Checking initial elements" << std::endl;
        for (int i = 0; i < 1000; i++)
        {
            int pos = mt_gen() % INIT_SIZE;
            auto op = ops[pos];
            assert(op.first == 0);
            assert(list.find(op.second));
        }
        assert(!list.find(5));
        assert(!list.find(MAX / 2));
        assert(!list.find(MAX + 10));
        std::cout << " --- End of initial test --- " << std::endl;
    }

    // multiple threads
    {
        std::cout << " --- Multi test --- " << std::endl;
        std::atomic<int> size(0);
        size.store(list.size());

        auto thread_func = [&](int id)
        {
            auto seed = time_seed * 1000 + id;
            auto gen = OperationGenerator(seed, 10, MAX, 50);
            int count = (TOTAL_SIZE - INIT_SIZE) / thread_count;

            for (int i = 0; i < count; i++)
            {
                Operation op = gen.next();
                if (i % 5000 == 0)
                {
                    std::cout << "Thread " << id << " : " << i << " / " << count << " : " << op.first << " " << op.second << std::endl;
                }
                if (op.first == 0)
                {
                    bool success = list.insert(op.second);
                    if (success)
                    {
                        size.fetch_add(1);
                    }
                }
                else
                {
                    bool success = list.erase(op.second);
                    if (success)
                    {
                        size.fetch_sub(1);
                    }
                }
            }
        };

        std::cout << "Starting threads" << std::endl;
        auto start = std::chrono::high_resolution_clock::now();
        std::vector<std::thread> threads;
        for (int i = 0; i < thread_count; i++)
        {
            threads.push_back(std::thread(thread_func, i));
        }

        for (auto &t : threads)
        {
            t.join();
        }
        auto end = std::chrono::high_resolution_clock::now();
        std::cout << "Elapsed time: " << std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() << "ms" << std::endl;

        std::cout << "Checking list size" << std::endl;
        std::cout << "List size (tracked) " << size.load() << std::endl;
        std::cout << "List size (real): " << list.size() << std::endl;
        assert(size.load() == list.size());

        // reconstruct operations to check
        std::cout << "Reconstructing operations" << std::endl;
        for (int tid = 0; tid < thread_count; tid++)
        {
            auto seed = time_seed * 1000 + tid;
            auto gen = OperationGenerator(seed, 10, MAX, 50);
            int count = (TOTAL_SIZE - INIT_SIZE) / thread_count;
            for (int i = 0; i < count; i++)
            {
                ops.push_back(gen.next());
                op_set.insert(ops.back());
            }
        }

        // check ops set

        std::cout << " --- End of multi test --- " << std::endl;
    }

    // print
    std::cout << " --- End of all tests --- " << std::endl;
}

int main()
{
    // assert(false);

    multi_test(8);
    return 0;

    HarrisList list;

    // Basic Tests for insert, erase and find

    list.insert(1);
    list.insert(2);
    list.insert(3);

    std::cout << list.find(1) << std::endl;
    std::cout << list.find(2) << std::endl;
    std::cout << list.find(3) << std::endl;
    std::cout << list.find(4) << std::endl;
    list.print();

    list.erase(2);
    std::cout << list.find(2) << std::endl;
    list.print();

    return 0;
}