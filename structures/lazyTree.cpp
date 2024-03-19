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
#include <queue>

struct Node;
typedef std::atomic<Node *> Edge;
typedef std::tuple<int, int, int> Operation; // key, value, type

struct Node
{
    std::atomic<bool> removed;
    std::mutex tree_mtx;

    std::atomic<int> sum;
    std::queue<Operation> op_queue;
    std::mutex op_mutex;

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

// make it work in serial setting
// maintaining min-max for returning early?

struct LeafTree
{
    const int MAX_KEY = 2147483647;
    InternalNode *root; // root will only have left child.
    LeafTree() : root(new InternalNode(MAX_KEY)) {}

    void propagate(InternalNode *nd)
    {
        // propagate one level
        // assume all locks are acquired properly
        while (!nd->op_queue.empty())
        {
            auto [key, val, type] = nd->op_queue.front();
            nd->op_queue.pop();
            int dir = nd->key <= key;
            Node *child = nd->child[dir].load();
            if (child == nullptr)
                continue; // can this happen?

            child->op_mutex.lock();
            child->op_queue.push({key, val, type});
            child->sum.fetch_add(val * type);
            child->op_mutex.unlock();
        }
    }

    auto find(InternalNode *root, int key)
    {
        // does not propagate operations
        // for insert & remove
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
            l = p->child[p_dir].load();
        }

        return std::make_tuple(gp, gp_dir, p, p_dir, (LeafNode *)l);
    }

    bool insert(InternalNode *root, int key, int val)
    {
        // TODO
        while (true)
        {
            auto [gp, gp_dir, p, p_dir, leaf] = find(root, key);
            if (leaf->key == key)
                return false;

            p->tree_mtx.lock();
            Edge *ptr = &(p->child[p_dir]); // desired location
            if (p->removed.load() || ptr->load() != leaf)
            {
                // p updated
                p->tree_mtx.unlock();
                continue;
            }

            LeafNode *new_leaf_node = new LeafNode(key, val);
            InternalNode *new_in_node =
                (leaf->key < key)
                    ? new InternalNode(key, leaf, new_leaf_node)
                    : new InternalNode(leaf->key, new_leaf_node, leaf);
            root->op_mutex.lock();
            root->op_queue.push({key, val, 1});
            root->sum.fetch_add(val);
            root->op_mutex.unlock();
            // check size of root and propagate (just before return) if full
            ptr->store(new_in_node);

            p->tree_mtx.unlock();
            return true;
        }
    };

    bool remove(InternalNode *root, int key)
    {
        // TODO
        LeafNode *prev_leaf = nullptr;
        while (true)
        {
            auto [gp, gp_dir, p, p_dir, leaf] = find(root, key);
            if (leaf->key != key)
                return false; // key not found
            if (prev_leaf != nullptr && prev_leaf != leaf)
                return false; // key deleted and re-added
            prev_leaf = leaf;

            gp->tree_mtx.lock();
            p->tree_mtx.lock();
            Edge *ptr = &(gp->child[gp_dir]);
            if (gp->removed.load() || ptr->load() != p)
            {
                gp->tree_mtx.unlock();
                p->tree_mtx.unlock();
                continue;
            }
            Node *remaining_leaf = p->child[1 - p_dir].load();
            Node *target_leaf = p->child[p_dir].load();
            if (target_leaf != leaf)
            {
                gp->tree_mtx.unlock();
                p->tree_mtx.unlock();
                continue;
            }

            root->op_mutex.lock();
            root->op_queue.push({key, leaf->value, -1});
            root->sum.fetch_sub(leaf->value);
            root->op_mutex.unlock();

            p->removed.store(true);
            ptr->store(remaining_leaf);

            // TODO remove nodes
            // delete p;
            // delete l;

            gp->tree_mtx.unlock();
            p->tree_mtx.unlock();
            return true;
        }
    }

    int sum(InternalNode *root, int key_st, int key_ed)
    {
        // sum for [key_st, key_ed]

        Node *nd = root;
        // TODO lock
        while (nd != nullptr && !nd->is_leaf)
        {
            // nd->tree_mtx.lock();
            // nd->tree_mtx.unlock();
            // TODO propagate, move lock
            propagate((InternalNode *)nd);
            bool st_dir = nd->key <= key_st;
            bool ed_dir = nd->key <= key_ed;
            if (st_dir == ed_dir)
                nd = ((InternalNode *)nd)->child[st_dir].load();
            else
                break;
        }

        if (nd == nullptr)
            return 0;

        if (nd->is_leaf)
        {
            if (key_st <= nd->key && nd->key <= key_ed)
                return ((LeafNode *)nd)->value;
            else
                return 0;
        }

        InternalNode *sub_root = (InternalNode *)nd;
        // TODO lock

        int result = 0;
        int keys[2] = {key_st, key_ed};
        std::queue<std::pair<Node *, int>> q;
        q.push({sub_root->child[0], 0});
        q.push({sub_root->child[1], 1});
        while (!q.empty())
        {
            auto [nd, r_dir] = q.front();
            q.pop();
            if (nd->is_leaf)
            {
                if (key_st <= nd->key && nd->key <= key_ed)
                    result += ((LeafNode *)nd)->value;
            }
            else
            {
                InternalNode *cur = (InternalNode *)nd;
                propagate(cur);
                // TODO propagate, move lock
                int key = keys[r_dir];
                int dir = nd->key <= key;

                // if I should go to r_dir, add the other subtree sum. if not, just go.
                if (r_dir == dir)
                {
                    Node *other_subtree = cur->child[1 - dir].load();
                    result += (other_subtree == nullptr) ? 0 : other_subtree->sum.load();
                }
                q.push({cur->child[dir], r_dir});
            }
        }

        // TODO unlock everything
        return result;
    }

    //
    bool search(InternalNode *root, int key)
    {
        Node *nd = root->child[0].load();
        while (!nd->is_leaf)
        {
            auto nd_child = ((InternalNode *)nd)->child;
            Edge *ptr = (key < nd->key) ? &(nd_child[0]) : &(nd_child[1]);
            nd = ptr->load();
        }

        auto leaf = (LeafNode *)nd;
        return leaf->key == key && !leaf->removed.load();
    }
};
