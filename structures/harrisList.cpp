#include <atomic>
#include <iostream>
#include <utility>

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
};

typedef std::pair<Node *, Node *> NodePair;

// Questions:
// atomic guarantees that cas, read and write is consistent? no need to make it volatile?
// is mark also included as a part of CAS? is it compared properly?
// what if delete and insert contends with same key? does marking work ok?
// should I deliberately free the nodes? (I should, right?)
// ref-count list?
// lock-free vs deadlock-free?

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

    NodePair search(int key)
    {
        Node *t = head;
        Node *t_next = head->next.load().ptr;

        // Find left_node and right_node
        do
        {

        } while (true);
    }
};

int main()
{
    return 0;
}