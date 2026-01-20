"""
B+ tree implementation and basic doctests.
to run Doctests, use:
***
python -m doctest bplustreelib/bplustree.py -v
***

Module-level doctests:

>>> t = BPlusTree(order=4)
>>> t.insert(10, 'a')
True
>>> t.insert(20, 'b')
True
>>> t.insert(30, 'c')
True
>>> t.search(10)
'a'
>>> t.search(20)
'b'
>>> t.search(99) is None
True
>>> # Duplicate insert returns False
>>> t.insert(20, 'x')
False
>>> list(t.traverse())
[(10, 'a'), (20, 'b'), (30, 'c')]
>>> t.range_search(15, 30)
[(20, 'b'), (30, 'c')]
>>> # Insert one more to force a split (order=4 -> capacity 3)
>>> t.insert(40, 'd')
True
>>> t.root.is_leaf
False
>>> [t.search(k) for k in (10,20,30,40)]
['a', 'b', 'c', 'd']
>>> # Delete an existing key and a non-existing one
>>> t.delete(20)
True
>>> t.search(20) is None
True
>>> t.delete(99)
False
>>> d = t.to_dict()
>>> t2 = BPlusTree.from_dict(d)
>>> [t2.search(k) for k in (10,30,40)]
['a', 'c', 'd']
"""

import json

class BPlusTreeNode:
    """
    A node in the B+ tree. Can be either a leaf node (storing key-value pairs)
    or an internal node (storing keys and child pointers).

    Attributes:
        is_leaf (bool): True if this is a leaf node, False if internal
        keys (list): List of keys stored in this node
        values (list): List of values (only used in leaf nodes)
        children (list): List of child node pointers (only used in internal nodes)
        next (BPlusTreeNode): Link to next leaf node (only used in leaf nodes)
    """
    def __init__(self, is_leaf=False):
        self.is_leaf = is_leaf
        self.keys = []
        self.values = []      # for leaf nodes
        self.children = []    # for internal nodes
        self.next = None      # link to next leaf


class BPlusTree:
    def __init__(self, order=4):
        """
        Initialize an empty B+ tree.
        
        Args:
            order (int): The order of the B+ tree. Determines the maximum number of children
                        per node and key-value pairs per leaf. Default is 4.
        """
        self.root = BPlusTreeNode(is_leaf=True)
        self.order = order

    def search(self, key):
        """
        Search for a specific key in the B+ tree.
        
        Args:
            key: The key to search for
            
        Returns:
            The value associated with the key if found, None otherwise
        """
        leaf = self._find_leaf(key)
        for atIndex, k in enumerate(leaf.keys):
            if k == key:
                return leaf.values[atIndex]
        return None #Key does not exist


    def insert(self, key, value):
        """
        Insert a key-value pair into the B+ tree.
        
        Args:
            key: The key to insert
            value: The value associated with the key
            
        Returns:
            True if insertion was successful, False if key already exists
        """
        leaf = self._find_leaf(key)

        # Check if key already exists
        for k in leaf.keys:
            if k == key:
                return False  # Key already exists
            
        # Insert key-value pair into leaf node
        insert_index = 0
        while insert_index < len(leaf.keys) and leaf.keys[insert_index] < key:
            insert_index += 1
        leaf.keys.insert(insert_index, key)
        leaf.values.insert(insert_index, value)

        # Check for overflow and split if necessary
        if len(leaf.keys) > self.order - 1:
            self._split_leaf(leaf)
        return True

    def delete(self, key):
        """
        Delete a key and its associated value from the B+ tree.
        
        Args:
            key: The key to delete
            
        Returns:
            True if the key was found and deleted, False otherwise
        """
        leaf = self._find_leaf(key)
        if key not in leaf.keys:
            return False  # Key not found

        # Remove key-value pair from leaf node
        index = leaf.keys.index(key)
        leaf.keys.pop(index)
        leaf.values.pop(index)

        # Check for underflow and merge if necessary
        if len(leaf.keys) < (self.order - 1) // 2:
            self._merge_nodes(leaf)

        return True
    
    def modify(self, key, mode, new_value):
        """
        Modify the value stored at a given key.

        Args:
            key: The key whose value is to be modified
            mode: 'change' to overwrite the value, 'append' to add the new_value to a List-like value
            new_value: The new value to set or append

        Returns:
            True on success, False if key not found or mode invalid

        Examples:
        >>> t = BPlusTree(order=4)
        >>> t.insert(1, 'x')
        True
        >>> t.modify(1, 'change', 'y')
        True
        >>> t.search(1)
        'y'
        >>> t.modify(1, 'append', 'z')
        True
        >>> t.search(1)
        ['y', 'z']
        >>> # modify non-existent key
        >>> t.modify(2, 'change', 'n')
        False
        >>> # invalid mode
        >>> t.modify(1, 'invalid', 'n')
        False
        """
        leaf = self._find_leaf(key)
        if key not in leaf.keys:
            return False  # Key not found

        index = leaf.keys.index(key)
        if mode == 'change':
            leaf.values[index] = new_value
        elif mode == 'append':
            if isinstance(leaf.values[index], list):
                leaf.values[index].append(new_value)
            else:
                leaf.values[index] = [leaf.values[index], new_value]
        else:
            return False  # Invalid mode

        return True

    def range_search(self, start_key, end_key):
        """
        Find all key-value pairs where key is between start_key and end_key inclusive.
        
        Args:
            start_key: The lower bound of the range (inclusive)
            end_key: The upper bound of the range (inclusive)
            
        Returns:
            List of (key, value) tuples within the specified range
        """
        # Find starting leaf
        results = []
        leaf = self._find_leaf(start_key)
        # Iterate leaf nodes until keys exceed end_key
        while leaf:
            for k, v in zip(leaf.keys, leaf.values):
                if k < start_key:
                    continue
                if k > end_key:
                    return results
                results.append((k, v))
            leaf = leaf.next
        return results

    def _find_leaf(self, key):
        """
        Find the leaf node where the key should be located.
        
        Args:
            key: The key to search for
            
        Returns:
            The leaf node where the key should be located
        """
        curr = self.root
        while not curr.is_leaf:
            i = 0
            while i < len(curr.keys) and key >= curr.keys[i]:
                i += 1
            curr = curr.children[i]

        return curr

    def _find_parent(self, curr, child):
        """
        Find the parent of `child` starting from `curr` node.

        Args:
            curr: Node to start searching from (usually root)
            child: Node whose parent to find

        Returns:
            The parent node if found, otherwise None
        """
        # If curr is leaf, it cannot be parent
        if curr is None or curr.is_leaf:
            return None
        for c in curr.children:
            if c is child:
                return curr
        # Recurse into children
        for c in curr.children:
            if not c.is_leaf:
                parent = self._find_parent(c, child)
                if parent is not None:
                    return parent
        return None

    def _split_leaf(self, node):
        """
        Split a leaf node that has reached maximum capacity.
        
        Args:
            node: The leaf node to split
            
        Returns:
            The new right node and the middle key
        """
        mid_index = len(node.keys) // 2
        new_leaf = BPlusTreeNode(is_leaf=True)
        
        # Move half the keys and values to the new leaf
        new_leaf.keys = node.keys[mid_index:]
        new_leaf.values = node.values[mid_index:]
        node.keys = node.keys[:mid_index]
        node.values = node.values[:mid_index]
        
        # Update leaf links
        new_leaf.next = node.next
        node.next = new_leaf
        
        # Promote the first key of the new leaf to the parent
        if node == self.root:
            # Create a new root
            new_root = BPlusTreeNode(is_leaf=False)
            new_root.keys = [new_leaf.keys[0]]
            new_root.children = [node, new_leaf]
            self.root = new_root
        else:
            # Find parent and insert the promoted key
            parent = self._find_parent(self.root, node)
            insert_index = 0
            while insert_index < len(parent.keys) and parent.keys[insert_index] < new_leaf.keys[0]:
                insert_index += 1
            parent.keys.insert(insert_index, new_leaf.keys[0])
            parent.children.insert(insert_index + 1, new_leaf)
            
            # Check for overflow in parent
            if len(parent.keys) > self.order - 1:
                self._split_internal(parent)

    def _split_internal(self, node):
        """
        Split an internal node that has reached maximum capacity.
        
        Args:
            node: The internal node to split
            
        Returns:
            The new right node and the middle key
        """
        # Determine middle index and promoted key
        mid_index = len(node.keys) // 2
        promoted_key = node.keys[mid_index]

        # Create new right internal node
        new_node = BPlusTreeNode(is_leaf=False)

        # Right node takes keys after the promoted key
        new_node.keys = node.keys[mid_index + 1:]

        # Children split: left keeps children up to mid_index, right takes the rest
        # For internal nodes with k keys there are k+1 children
        new_node.children = node.children[mid_index + 1:]

        # Left (existing) node keeps keys before mid_index and corresponding children
        node.keys = node.keys[:mid_index]
        node.children = node.children[:mid_index + 1]

        # If splitting the root, create a new root
        if node == self.root:
            new_root = BPlusTreeNode(is_leaf=False)
            new_root.keys = [promoted_key]
            new_root.children = [node, new_node]
            self.root = new_root
        else:
            # Find parent and insert promoted key and new child
            parent = self._find_parent(self.root, node)
            insert_index = 0
            while insert_index < len(parent.keys) and parent.keys[insert_index] < promoted_key:
                insert_index += 1
            parent.keys.insert(insert_index, promoted_key)
            parent.children.insert(insert_index + 1, new_node)

            # If parent now overflows, split it too
            if len(parent.keys) > self.order - 1:
                self._split_internal(parent)

    def _merge_nodes(self, left, right=None, parent=None, parent_index=None):
        """
        Merge two sibling nodes when deletion causes an underflow.

        This method supports two calling styles:
        - _merge_nodes(node): Node is the one that underflowed; the method
          will locate its parent and a sibling and merge appropriately.
        - _merge_nodes(left, right, parent, parent_index): Explicitly provide
          the two siblings, their parent, and the index of the left child in
          parent's children list.

        Args:
            left: The left node to merge (or the underflowing node when called
                  with a single argument)
            right: The right sibling node (optional)
            parent: The parent node (optional)
            parent_index: Index of the left node in parent's children (optional)

        Returns:
            True if merge was performed, False otherwise
        """

        # Allow being called with a single argument: the node that underflowed.
        # In that case, find its parent and a sibling to merge with.
        if right is None:
            node = left
            parent = self._find_parent(self.root, node)
            # If there is no parent, node is root — nothing to merge
            if parent is None:
                return False

            idx = parent.children.index(node)
            # Prefer merging with right sibling if available
            if idx + 1 < len(parent.children):
                left = node
                right = parent.children[idx + 1]
                parent_index = idx
            elif idx - 1 >= 0:
                left = parent.children[idx - 1]
                right = node
                parent_index = idx - 1
            else:
                # No siblings to merge with
                return False

        # Now left, right, parent, parent_index should be set
        if left.is_leaf:
            # Merge right into left for leaf nodes
            left.keys.extend(right.keys)
            left.values.extend(right.values)
            left.next = right.next

            # Remove separator key and right child from parent
            if parent is not None and parent_index is not None and parent_index < len(parent.keys):
                parent.keys.pop(parent_index)
            elif parent is not None and parent_index is not None and parent_index == len(parent.keys):
                # If merging with the last child, remove the last key
                parent.keys.pop()
            if parent is not None:
                parent.children.pop(parent_index + 1)
        else:
            # Internal node merge: bring down separator key from parent
            sep_key = parent.keys[parent_index]
            left.keys.append(sep_key)
            left.keys.extend(right.keys)
            left.children.extend(right.children)

            # Remove separator and right child from parent
            parent.keys.pop(parent_index)
            parent.children.pop(parent_index + 1)

        # If parent is root and becomes empty, make left the new root
        if parent == self.root and len(parent.keys) == 0:
            self.root = left
            return True

        # If parent underflows, try to fix by merging up the tree
        if parent is not None and len(parent.keys) < (self.order - 1) // 2:
            # Find parent's parent and attempt to merge parent upward
            parent_parent = self._find_parent(self.root, parent)
            if parent_parent is None:
                # parent is root; if empty we've already handled, otherwise nothing
                if len(parent.keys) == 0:
                    self.root = parent.children[0]
                return True
            p_idx = parent_parent.children.index(parent)
            # Prefer merging parent with right sibling
            if p_idx + 1 < len(parent_parent.children):
                self._merge_nodes(parent, parent_parent.children[p_idx + 1], parent_parent, p_idx)
            else:
                self._merge_nodes(parent_parent.children[p_idx - 1], parent, parent_parent, p_idx - 1)

        return True

    def traverse(self):
        """
        Traverse the B+ tree in order.
        
        Returns:
            Generator yielding (key, value) pairs in ascending order of keys
        """
        current = self.root
        # Go to the leftmost leaf
        while not current.is_leaf:
            current = current.children[0]
        
        # Traverse leaf nodes
        while current:
            for k, v in zip(current.keys, current.values):
                yield (k, v)
            current = current.next

    def print_tree(self):
        """
        Print a visual representation of the B+ tree structure.
        Useful for debugging and visualization.
        """
        def _print_node(node, level):
            print("  " * level + ("Leaf: " if node.is_leaf else "Internal: ") + str(node.keys))
            if not node.is_leaf:
                for child in node.children:
                    _print_node(child, level + 1)

        _print_node(self.root, 0)

    # -----------------
    # Serialization (JSON only, no pickle)
    # -----------------
    def to_dict(self):
        """Return a JSON-serializable dict representing the tree."""
        def node_to_dict(node):
            nd = {"is_leaf": node.is_leaf, "keys": list(node.keys)}
            if node.is_leaf:
                nd["values"] = list(node.values)
            else:
                nd["children"] = [node_to_dict(c) for c in node.children]
            return nd

        return {"order": self.order, "root": node_to_dict(self.root)}

    def to_json(self):
        """Return a JSON string for the tree."""
        return json.dumps(self.to_dict())

    @classmethod
    def from_dict(cls, d):
        """Reconstruct a BPlusTree from a dictionary produced by to_dict()."""
        def dict_to_node(nd):
            node = BPlusTreeNode(is_leaf=nd.get("is_leaf", False))
            node.keys = list(nd.get("keys", []))
            if node.is_leaf:
                node.values = list(nd.get("values", []))
            else:
                node.children = [dict_to_node(c) for c in nd.get("children", [])]
            return node

        tree = cls(order=d.get("order", 4))
        tree.root = dict_to_node(d["root"]) if d.get("root") is not None else BPlusTreeNode(is_leaf=True)

        # Reconstruct leaf next pointers by collecting leaves left-to-right
        leaves = []
        def collect_leaves(node):
            if node.is_leaf:
                leaves.append(node)
            else:
                for c in node.children:
                    collect_leaves(c)

        collect_leaves(tree.root)
        for i in range(len(leaves) - 1):
            leaves[i].next = leaves[i + 1]
        if leaves:
            leaves[-1].next = None

        return tree

    @classmethod
    def from_json(cls, s):
        """Load a tree from a JSON string previously returned by to_json()."""
        return cls.from_dict(json.loads(s))
