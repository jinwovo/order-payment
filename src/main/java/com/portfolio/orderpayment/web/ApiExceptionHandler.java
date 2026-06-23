package com.portfolio.orderpayment.web;

import com.portfolio.orderpayment.catalog.UnknownProductException;
import com.portfolio.orderpayment.ordering.OrderNotFoundException;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.MissingRequestHeaderException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
public class ApiExceptionHandler {

    public record ApiError(String error, String message) {
    }

    @ExceptionHandler(OrderNotFoundException.class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    public ApiError notFound(OrderNotFoundException e) {
        return new ApiError("ORDER_NOT_FOUND", e.getMessage());
    }

    @ExceptionHandler(UnknownProductException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ApiError unknownProduct(UnknownProductException e) {
        return new ApiError("UNKNOWN_PRODUCT", e.getMessage());
    }

    @ExceptionHandler({MethodArgumentNotValidException.class, MissingRequestHeaderException.class})
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ApiError badRequest(Exception e) {
        return new ApiError("BAD_REQUEST", e.getMessage());
    }
}
