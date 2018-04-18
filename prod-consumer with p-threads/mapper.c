#include<stdio.h>
#include <string.h>
#include <pthread.h>
#define MAXCHAR 10000
struct Tuples {
  
  char userID[5];
  char topic[16];
  int score;
};

struct Tuples buffer[MAXCHAR]={0};
struct Tuples **buffer1;
pthread_mutex_t buf_lock;
pthread_cond_t buf_full[500],buf_empty[500];


int count=0, *in, *out,done[100];
int size_of_buff=0,num_red_threads;
char* filename;
void *consumer(void *param);
void *producer(void *param);

FILE *fptr;






int main(int argc, char *argv[]) {


int i=0;

num_red_threads= atoi(argv[2]);
size_of_buff=atoi(argv[1]);
buffer1 = (struct Tuples **)malloc(sizeof(struct Tuples *) * num_red_threads);
    *buffer1 = (struct Tuples *)malloc(sizeof(struct Tuples) * num_red_threads * size_of_buff);

in = (int*) malloc(num_red_threads * sizeof(int));
out = (int*) malloc(num_red_threads * sizeof(int));

    for(i = 0; i < num_red_threads; i++)
        buffer1[i] = (*buffer1 + size_of_buff * i);

   for(i=0; i<num_red_threads; i++)
	{
	in[i]=0;
	out[i]=0;
	}

fptr = fopen("output.txt","w");
pthread_t producer_thread[1];
pthread_t consumer_threads[num_red_threads];

pthread_mutex_init(&buf_lock, NULL);
	for(i=0;i<num_red_threads;i++){
 pthread_cond_init(&buf_full[i], NULL);		/* Initialize consumer condition variable */
  pthread_cond_init(&buf_empty[i], NULL);}
 
   pthread_create(&producer_thread[1], NULL, producer,NULL);
 
	for(i=0;i<num_red_threads;i++)
	{
	pthread_create(&consumer_threads[i], NULL, consumer, (void *)i);
	}
 

  pthread_join(producer_thread[1], NULL);
 
  for (i = 0; i < num_red_threads; i++) 
  {
    pthread_join(consumer_threads[i], NULL);
  }
 
 
 pthread_mutex_destroy(&buf_lock);
	for(i=0;i<num_red_threads;i++){
 pthread_cond_destroy(&buf_full[i]);
 pthread_cond_destroy(&buf_empty[i]);}
 pthread_exit (NULL);



}


void *consumer(void *param)
{
 int tid;
   tid=(int)param;
const char s[4] = "()";
  const char newLine[4] = "\n";
  const char comma[2] = ",";
  char *token;
  struct Tuples inputTuple[10000]={0};
  int j = 0, i = 0, init = 0, k = 0, addVal = 0;
  char str[MAXCHAR];
  char *userId;
  char *topic;
  char *score;
  
  int index = 0,buff_index=0;

	while(1){
	pthread_mutex_lock (&buf_lock);

	while((in[tid]-out[tid])==0)// buffer empty
	{
	  printf("\nIn consumer waiting to for data to be entered:\n");
	  printf("\nEmpty buffer for thread %d \n", tid);
    pthread_cond_wait(&buf_empty[tid], &buf_lock);	
	printf("\nwait ends for tid %d",tid);
	}
	
  while ((in[tid]-out[tid])>0) { 
   userId = buffer1[tid][buff_index].userID;
    topic = buffer1[tid][buff_index].topic;
    score = buffer1[tid][buff_index].score;
    printf("\nIn consumer \ttid :%d %s %s %d",tid,userId,topic,score);
	for(i=0;i<4;i++)
    inputTuple[buff_index].userID[i]=buffer1[tid][buff_index].userID[i];
    for(i=0;i<15;i++)
    {inputTuple[buff_index].topic[i]=buffer1[tid][buff_index].topic[i];}
    
    inputTuple[buff_index].score=buffer1[tid][buff_index].score;

     

	index++;
    buff_index++;
    out[tid]++;



}
	





        for(i=0;i<index;i++)
      {
    	for(k=i+1;k<index;k++)
    	{ 
              if((strcmp(inputTuple[i].userID, inputTuple[k].userID)==0)&& (strcmp(inputTuple[i].topic, inputTuple[k].topic)==0))
          {   
              addVal=inputTuple[i].score+inputTuple[k].score;
               
              inputTuple[i].score=addVal;
              
          for(j=k;j<index-1;j=j+1)
          {   
               inputTuple[j]=inputTuple[j+1];
          }

          index=index-1;
          k=k-1;
          }    

       
    	} 

      }
	printf("\n");
for (i = 0; i < index; i = i +1 ) {
    
    printf("\nProcessed tuple:\t(%s,%s,%d)", inputTuple[i].userID, inputTuple[i].topic, inputTuple[i].score);
    fprintf(fptr,"\n(%s,%s,%d)", inputTuple[i].userID, inputTuple[i].topic, inputTuple[i].score);
  }	
	out[tid]=0;
	index=0;
	buff_index=0;
	in[tid]=0;
	

	if(done[tid]==1)
	{
	pthread_mutex_unlock (&buf_lock);
	break;
	}

pthread_cond_signal(&buf_full[tid]);
pthread_mutex_unlock (&buf_lock);

}
 pthread_exit(NULL);

}










void *producer(void *param){

    
    const char s[4] = "()";
     const char comma[2] = ",";
    char *token;
    FILE *fp,*fw;
    int j=0,i,init=0,loop=0,counter=0, global_buff_size=size_of_buff,buff_size=0;   
    char *wordsToken[500];
    char *eachWord[500];
    char *userID[500],*topic[500],*action[500],*uniqueIds[400];
    char source[MAXCHAR];

      struct Tuples buffer[MAXCHAR]={0};
    
     
     long buffer_size = 0;
   
    fgets(source, MAXCHAR, stdin);
    token = strtok(source, s);



 while( token != NULL ) {

      wordsToken[j++] = token;
         
      token = strtok(NULL, s);
   }


    for( i = 0; i <j; i=i+2)
    {   init=0;
        token = strtok(wordsToken[i], comma);

        while( token != NULL ) {
      eachWord[init++] = token;
      token = strtok(NULL, comma);
       } 
	
	userID[counter]=eachWord[0];
	topic[counter]=eachWord[2];
	action[counter]=eachWord[1];
	
	counter++;
	}
	init=0;
	
	for(i=0;i<counter;i++)
	{
	 for (j=0; j<i; j++)
	 {
	   if (strcmp(userID[j], userID[i])==0)
               break;	
	 }
	   if (i == j)
         { 
	 uniqueIds[init]=userID[i];
	 init++;

	}
	}
	

	for(i=0;i<num_red_threads;i++)
	 {
	buff_size=0;
	
	 for(j=0;j<counter;j++)
	 {
         if(strcmp(uniqueIds[i], userID[j])==0)
	  { pthread_mutex_lock (&buf_lock);
	    
	   if(in[i]==size_of_buff)
	   { pthread_cond_signal(&buf_empty[i]);
	    printf("\n\nproducer() buffer full for id:%s and in pointer is:%d",userID[j],in[i]);
	     pthread_cond_wait(&buf_full[i], &buf_lock);//wait if buffer is full 	
		in[i]=0;
		buff_size=0;

	    }

           if(in[i] < global_buff_size)
		{
	in[i]=in[i]+1;	
	   for(loop=0;loop<4;loop++)
      buffer1[i][buff_size].userID[loop]=userID[j][loop];	  
            for(loop=0;loop<15;loop++)
      buffer1[i][buff_size].topic[loop]=topic[j][loop];
	 if(strcmp("P", action[j])==0)
	buffer1[i][buff_size].score=50;

	 if(strcmp("L", action[j])==0)
	buffer1[i][buff_size].score=20;

	if(strcmp("D", action[j])==0)
	buffer1[i][buff_size].score=-10;	

	if(strcmp("C", action[j])==0)
	buffer1[i][buff_size].score=30;

	if(strcmp("S", action[j])==0)
	buffer1[i][buff_size].score=40;
	 printf("\n In Producer \t buffer in:%d %d %s %s %d ",in[i],buff_size,buffer1[i][buff_size].userID,buffer1[i][buff_size].topic,buffer1[i][buff_size].score);
	 buff_size++; 
	 
	pthread_mutex_unlock (&buf_lock);
        }
	 
	 }
	 }

       	 pthread_cond_signal(&buf_empty[i]);
         done[i]=1;
	printf("\nTuples for buffer %d over",i);
	 
	 }	
	

   	 pthread_exit(NULL);
}
