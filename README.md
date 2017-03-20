# SQLAPI

Node.js library. Experimental effort to create a database driver abstraction
layer for Node.js.

Driver implementations can register themselves with SQLAPI under a certain
name, and a function is provided to allow connections to be opened.

Opinions:

  - Total use of Promises. There will be no synchronous functions, or functions
    taking callback arguments.

