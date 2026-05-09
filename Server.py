import os

mode = os.getenv('CHAT_SERVER_MODE', 'multi_processing').strip().lower()

if mode in {'thread', 'threads', 'multi_threading', 'multi-threading', 'single', 'single_node', 'single-node'}:
    from chatapp_core.multi_threading_server import main
else:
    from chatapp_core.multi_processing_server import main

if __name__ == '__main__':
    main()
