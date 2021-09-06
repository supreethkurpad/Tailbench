#ifndef __INTERNAL_H
#define __INTERNAL_H

class AsrException : public std::exception {
    private:
        std::string msg;
    public:
        AsrException(std::string msg) : msg(msg) {}
        ~AsrException() throw() {}
        virtual const char* what() const throw() {
            return msg.c_str();
        }
};

#endif
