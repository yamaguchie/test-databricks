
import argparse

def main():
    # argparse.ArgumentParserクラスをインスタンス化して、説明等を引数として渡す
    parser = argparse.ArgumentParser(
        prog="sample",  # プログラム名
        usage="python main.py <file_name> <password>", # プログラムの利用方法
        description="sample script.", # ヘルプの前に表示
        epilog="end", # ヘルプの後に表示
        add_help=True, # -h/–-helpオプションの追加
    )
    
    # 引数の設定
    parser.add_argument("-f", "--file", 
    		    type=str, 
    		    default="hoge.pyAAAA",
    		    help="File name")
    
    parser.add_argument("-p", "--passwd", 
    		    type=str, 
    		    default="passwAAAd",
    		    help="Password")
    
    # 引数の解析
    args = parser.parse_args()
    
    print(f"File name: {args.file}, Password: {args.passwd}")

if __name__ == '__main__':
    main()