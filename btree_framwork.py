# Reference: https://www.tutorialspoint.com/python_data_structure/python_binary_tree.htm

# Create Root
class Node:
    def __init__(self, data) -> None:
        self.left = None
        self.right = None
        self.data = data
    
#     def PrintTree(self):
#         print(self.data)
    
# root = Node(10)
# root.PrintTree

# Insert Node
def insert(self, data):
      if self.data:
         if data < self.data:
            if self.left is None:
               self.left = Node(data)
            else:
               self.left.insert(data)
         else data > self.data:
            if self.right is None:
               self.right = Node(data)
            else:
               self.right.insert(data)
      else:
         self.data = data

# Print the Tree
   def PrintTree(self):
      if self.left:
         self.left.PrintTree()
      print( self.data),
      if self.right:
         self.right.PrintTree()

# Inorder traversal: Left to Root to Right
   def inorderTraversal(self, root):
      res = []
      if root:
         res = self.inorderTraversal(root.left)
         res.append(root.data)
         res = res + self.inorderTraversal(root.right)
      return res

# root = Node(27)
# root.insert(14)
# root.insert(35)
# root.insert(10)
# root.insert(19)
# root.insert(31)
# root.insert(42)
# print(root.inorderTraversal(root))