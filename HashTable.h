#include <iostream>

template<typename T>
struct Hbuckets
{
private:
   // Óçåë äëÿ õðàíåíèÿ äàííûõ
   template<typename U>
   struct Node
   {
       std::string key;       // Êëþ÷
       T value;          // Çíà÷åíèå
       Node<U>* next;    // Óêàçàòåëü íà ñëåäóþùèé óçåë

       Node(const std::string& key, const T& value) : key(key), value(value), next(nullptr) {}
   };

   Node<T>* head;    // Ãîëîâà ñïèñêà
   Node<T>* tail;    // Õâîñò ñïèñêà

public:
   Hbuckets() : head(nullptr), tail(nullptr) {}

   bool isEmpty()
   {
       return head == nullptr;
   }

   T get(const std::string& key)
   {
       Node<T>* tempNode = head;
       while (tempNode && tempNode->key != key) tempNode = tempNode->next;
       if (tempNode && tempNode->key == key)
           return tempNode->value;
       throw std::runtime_error("Key not found");
   }

   void push_back(const std::string& key, const T& value)
   {
       Node<T>* newNode = new Node<T>(key, value);
       if (isEmpty())
       {
           head = newNode;
           tail = newNode;
           return;
       }
       tail->next = newNode;
       tail = newNode;
   }

   void remove(const std::string& key)
   {
       if (isEmpty()) return;

       if (head->key == key)
       {
           Node<T>* temp = head;
           head = head->next;
           delete temp;

           if (head == nullptr) tail = nullptr;

           return;
       }

       Node<T>* prev = head;
       Node<T>* current = head->next;

       while (current != nullptr)
       {
           if (current->key == key)
           {
               prev->next = current->next;
               if (current == tail) tail = prev;
               delete current;
               return;
           }
           prev = current;
           current = current->next;
       }
   }

   void print()
   {
       if (isEmpty()) return;
       Node<T>* currentNode = head;
       while (currentNode)
       {
           std::cout << "[" << currentNode->key << ": " << currentNode->value << "] ";
           currentNode = currentNode->next;
       }
       std::cout << std::endl;
   }
};


template<typename T>
struct HASHtable
{
private:
   uint32_t capacity;
   Hbuckets<T>* arr;

   uint32_t hash(const std::string& value)
   {
       uint32_t hash = 5381;
       for (char c : value)
       {
           hash = ((hash << 5) + hash) + c;
       }
       return hash % capacity;
   }

public:
   HASHtable(int capacityValue)
   {
       arr = new Hbuckets<T>[capacityValue];  // Âûäåëÿåì ìàññèâ áàêåòîâ
       capacity = capacityValue;
   }

   // Ìåòîä âñòàâêè ýëåìåíòà
   void HSET(const std::string& key, const T& value)
   {
       uint32_t index = hash(key);  // Ïîëó÷àåì èíäåêñ
       arr[index].push_back(key, value);  // Äîáàâëÿåì ýëåìåíò â ñîîòâåòñòâóþùèé áàêåò
   }

   // Ìåòîä ïîëó÷åíèÿ ýëåìåíòà ïî êëþ÷ó
   T HGET(const std::string& key)
   {
       uint32_t index = hash(key);  // Ïîëó÷àåì èíäåêñ
       return arr[index].get(key);  // Èùåì ýëåìåíò â ñîîòâåòñòâóþùåì áàêåòå
   }

   
   void HDEL(const std::string& key)
   {
       uint32_t index = hash(key);  // Ïîëó÷àåì èíäåêñ
       arr[index].remove(key);  // Óäàëÿåì ýëåìåíò èç ñîîòâåòñòâóþùåãî áàêåòà
   }

   // Ïå÷àòü âñåõ áàêåòîâ
   void print()
   {
       for (uint32_t i = 0; i < capacity; ++i)
       {
           std::cout << "Bucket " << i << ": ";
           arr[i].print();
           std::cout << std::endl;
       }
   }

   ~HASHtable()
   {
       delete[] arr;
   }
};