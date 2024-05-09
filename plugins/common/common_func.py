"""
외부 파이썬 함수 실행 test     
"""
import random 

def generate_random():
    print(random.randint(0,10))

def op_args_test(name,age,*args):
    print(f"이름: {name}")
    print(f"나이: {age}")
    print(f"기타: {args}")
    
def op_kwargs_test(name, age, *args, **kwargs):
    print(f'이름: {name}')
    print(f'나이: {age}')
    print(f'기타: {args}')
    email = kwargs['email'] or None
    phone = kwargs.get('phone') or None
    if email:
        print(f"이메일: {email}")
    if phone:
        print(f"핸드폰: {phone}")
        
#op_kwargs_test("wj",19,"a","b","c",email="a",phone="010")