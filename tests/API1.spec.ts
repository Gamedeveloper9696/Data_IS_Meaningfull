import {test,expect, request, APIRequest} from '@playwright/test';


const url='https://6a0edf3f1736097c360abd9d.mockapi.io/person';

test('postreques',async({request})=>{

    const Response= await request.post(url,{data:{
        "name":"poornima",
        "job":"qa"
}});
        
console.log(await Response.json());

expect(Response.status()).toBe(201);
expect(await Response.json()).toMatchObject({
    "name":"poornima",
    "job":"qa"
});



    });
        
        
  


