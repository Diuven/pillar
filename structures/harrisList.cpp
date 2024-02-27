#include <atomic>
#include <iostream>
#include <utility>
#include <random>
#include <set>
#include <cassert>
#include <functional>
#include <thread>

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

    void print()
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
};

class LinearCongruentialGenerator
{
private:
    unsigned long a;
    unsigned long b;
    unsigned long m;
    unsigned long current;

public:
    LinearCongruentialGenerator(int a, int b, int m, int seed)
        : a(a), b(b), m(m), current(seed) {}

    int next()
    {
        current = (a * current + b) % m;
        return current;
    }
};

void multi_test(int thread_count)
{
    HarrisList list;

    const int MAX = 5e8;
    const int INIT_SIZE = 2e4;
    const int TOTAL_SIZE = 5e5;
    const int A = 48271, B = 3885;

    // std::set<int> keys;

    int time_seed = std::chrono::system_clock::now().time_since_epoch().count() % 1000000;
    std::cout << "Seed: " << time_seed << std::endl;
    auto mt_gen = std::mt19937(time_seed);
    std::uniform_int_distribution<int> distribution(1, MAX);

    // fill list with initial elements and test
    {
        std::cout << " --- Initial test --- " << std::endl;
        auto seed = time_seed * 1000 + (-1);
        auto gen = LinearCongruentialGenerator(A, B, MAX, seed);
        for (int i = 0; i < INIT_SIZE; i++)
        {
            int key = gen.next();
            // keys.insert(key);
            list.insert(key);
        }

        for (int i = 0; i < 1000; i++)
        {
            auto re_gen = LinearCongruentialGenerator(A, B, MAX, seed);
            int step = mt_gen() % INIT_SIZE;
            for (int j = 0; j < step; j++)
            {
                re_gen.next();
            }
            int key = re_gen.next();
            assert(list.find(key));
        }
        assert(!list.find(MAX + 10));
        assert(!list.find(MAX / 2));
        std::cout << " --- End of initial test --- " << std::endl;
    }

    // spawn insert threads
    auto thread_func = [&](int id)
    {
        auto seed = time_seed * 1000 + id;
        auto gen = LinearCongruentialGenerator(A, B, MAX, seed);
        int count = (TOTAL_SIZE - INIT_SIZE) / thread_count;

        // insert random elements
        for (int i = 0; i < count; i++)
        {
            if (i % 1000 == 0)
            {
                std::cout << "Thread " << id << " inserting " << i << "th element" << std::endl;
            }
            int key = gen.next();
            list.insert(key);
        }
    };

    std::cout << " --- Multi insert test --- " << std::endl;
    std::vector<std::thread> threads;
    for (int i = 0; i < thread_count; i++)
    {
        threads.push_back(std::thread(thread_func, i));
    }

    for (auto &t : threads)
    {
        t.join();
    }
    // test for single thread
    {
        std::cout << " --- Check multi insert --- " << std::endl;
        int tid = mt_gen() % thread_count;
        int seed = time_seed * 1000 + tid;
        for (int i = 0; i < 1000; i++)
        {
            auto re_gen = LinearCongruentialGenerator(A, B, MAX, seed);
            int step = mt_gen() % ((TOTAL_SIZE - INIT_SIZE) / thread_count);
            for (int j = 0; j < step; j++)
            {
                re_gen.next();
            }
            int key = re_gen.next();
            assert(list.find(key));
        }
        std::cout << " --- End of multi insert test --- " << std::endl;
    }

    // random insert, erase, find

    // print
    std::cout << " --- End of all tests --- " << std::endl;
}

int main()
{

    multi_test(16);
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