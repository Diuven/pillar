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
#include <string>
#include <mutex>

struct Node;
// typedef std::pair<Node *, int> Edge;
typedef std::atomic<Node *> Edge;

struct Node
{
    std::atomic<bool> removed;
    // std::atomic<int> sum;
    std::mutex mtx;

    int key;
    bool is_leaf;

    Node(int k = -1, bool is_leaf = false) : key(k), is_leaf(is_leaf), removed(false) {}
};

struct LeafNode : Node
{
    int value;
    LeafNode(int k, int v) : Node(k, true), value(v) {}
};

struct InternalNode : Node
{
    Edge child[2]; // [~, key), [key, ~)
    InternalNode(int k = -1, Node *l = nullptr, Node *r = nullptr) : Node(k, false)
    {
        child[0].store(l);
        child[1].store(r);
    }
};

// what is sentinal?

struct LeafTree
{
    InternalNode *root; // root does not have key, and will only have left child.
    LeafTree() : root(new InternalNode()) {}

    auto find(InternalNode *root, int key)
    {
        InternalNode *gp = nullptr;
        int gp_dir = 1;
        InternalNode *p = root;
        int p_dir = 0;
        Node *l = p->child[p_dir].load();

        while (!l->is_leaf)
        {
            gp = p;
            gp_dir = p_dir;
            p = (InternalNode *)l;
            p_dir = p->key <= key ? 1 : 0;
            l = p->child[p_dir].load(); // LinP for failed insert/delete : last load
        }

        return std::make_tuple(gp, gp_dir, p, p_dir, (LeafNode *)l);
    }

    bool insert(InternalNode *root, int key, int val)
    {
        // Node *prev_leaf = nullptr; // for upsert
        while (true)
        {
            auto [gp, gp_dir, p, p_dir, leaf] = find(root, key);
            if (leaf->key == key)
                return false;
            // prev_leaf = leaf;

            p->mtx.lock();
            Edge *ptr = &(p->child[p_dir]); // desired location
            if (p->removed.load() || ptr->load() != leaf)
            {
                // p updated
                p->mtx.unlock();
                continue;
            }

            LeafNode *new_leaf_node = new LeafNode(key, val);
            InternalNode *new_in_node =
                (leaf->key < key)
                    ? new InternalNode(key, leaf, new_leaf_node)
                    : new InternalNode(leaf->key, new_leaf_node, leaf);
            ptr->store(new_in_node); // LinP for success

            p->mtx.unlock();
            return true;
        }
    };

    bool remove(InternalNode *root, int key)
    {
        LeafNode *prev_leaf = nullptr;
        while (true)
        {
            auto [gp, gp_dir, p, p_dir, leaf] = find(root, key);
            if (leaf->key != key)
                return false; // key not found
            if (prev_leaf != nullptr && prev_leaf != leaf)
                return false; // key deleted and re-added
            prev_leaf = leaf;

            gp->mtx.lock();
            p->mtx.lock();
            Edge *ptr = &(gp->child[gp_dir]);
            if (gp->removed.load() || ptr->load() != p)
            {
                gp->mtx.unlock();
                p->mtx.unlock();
                continue;
            }
            Node *remaining_leaf = p->child[1 - p_dir].load();
            Node *target_leaf = p->child[p_dir].load();
            if (target_leaf != leaf)
            {
                gp->mtx.unlock();
                p->mtx.unlock();
                continue;
            }

            p->removed.store(true);
            ptr->store(remaining_leaf); // LinP for success

            // TODO remove nodes
            // delete p;
            // delete l;

            gp->mtx.unlock();
            p->mtx.unlock();
            return true;
        }
    }

    bool search(InternalNode *root, int key)
    {
        Node *nd = root->child[0].load();
        while (!nd->is_leaf)
        {
            auto nd_child = ((InternalNode *)nd)->child;
            Edge *ptr = (key < nd->key) ? &(nd_child[0]) : &(nd_child[1]);
            nd = ptr->load(); // LinP : last load
        }

        auto leaf = (LeafNode *)nd;
        return leaf->key == key;
    }
};
