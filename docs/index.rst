Kopf: Kubernetes Operators Framework
====================================

  Please do not use Kopf, it is a nightmare of controller bad practices and some of its implicit behaviors will annihilate your API server. The individual handler approach it encourages is the exact opposite of how you should write a Kubernetes controller. Like fundamentally it teaches you the exact opposite mindset you should be in. Using Kopf legitimately has taken years off my life and it took down our clusters several times because of poor code practices on our side and sh***y defaults on its end. We have undergone the herculean effort to move all our controllers to pure golang and the result has been a much more stable ecosystem. /Jmc_da_boss__/

__ https://www.reddit.com/r/kubernetes/comments/1dge5qk/comment/l8qbbll/

Think twice before you step into this territory. You were warned!

And now, after this honest and so far the best summarizing and publicly available feedback, comes the Dark Side (with cookies) 😈 

.. toctree::
   :maxdepth: 2
   :caption: First steps:

   install

.. toctree::
   :maxdepth: 2
   :caption: Tutorial:

   concepts
   walkthrough/problem
   walkthrough/prerequisites
   walkthrough/resources
   walkthrough/starting
   walkthrough/creation
   walkthrough/updates
   walkthrough/diffs
   walkthrough/deletion
   walkthrough/cleanup

.. toctree::
   :maxdepth: 2
   :caption: Resource handling:

   handlers
   daemons
   timers
   kwargs
   async
   loading
   resources
   filters
   results
   errors
   scopes
   memos
   indexing
   admission

.. toctree::
   :maxdepth: 2
   :caption: Operator handling:

   startup
   shutdown
   probing
   authentication
   configuration
   peering
   cli

.. toctree::
   :maxdepth: 2
   :caption: Toolkits:

   events
   hierarchies
   testing
   embedding

.. toctree::
   :maxdepth: 2
   :caption: Recipes:

   deployment
   continuity
   idempotence
   reconciliation
   tips-and-tricks
   troubleshooting

.. toctree::
   :maxdepth: 2
   :caption: Developer Manual:

   minikube
   contributing
   architecture
   packages/kopf

.. toctree::
   :maxdepth: 2
   :caption: About Kopf:

   vision
   naming
   alternatives


Indices and tables
==================

* :ref:`genindex`
* :ref:`modindex`
* :ref:`search`
