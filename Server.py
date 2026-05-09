
import os

if os.getenv('CHAT_SERVER_MODE', 'multi_processing') == 'multi_threading':
    from chatapp_core.multi_threading_server import main
else:
    from chatapp_core.multi_processing_server import main

if __name__ == '__main__':
    main()
