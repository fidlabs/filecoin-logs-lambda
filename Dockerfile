FROM public.ecr.aws/lambda/nodejs:14

# Copy function code  
COPY app.js package.json ${LAMBDA_TASK_ROOT}
RUN npm install

# Set the CMD to your handler
CMD [ "app.handler" ] 