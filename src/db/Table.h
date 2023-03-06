//
// Created by liu on 18-10-23.
//

#ifndef PROJECT_DB_TABLE_H
#define PROJECT_DB_TABLE_H

#include "../utils/formatter.h"
#include "../utils/uexception.h"
#include <algorithm>
#include <atomic>
#include <functional>
#include <iterator>
#include <limits>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#define _DBTABLE_ACCESS_WITH_NAME_EXCEPTION(field)                             \
  do {                                                                         \
    try {                                                                      \
      auto &index = table->fieldMap.at(field);                                 \
      return it->datum.at(index);                                              \
    } catch (const std::out_of_range &e) {                                     \
      throw TableFieldNotFound(R"(Field name "?" doesn't exists.)"_f %         \
                               (field));                                       \
    }                                                                          \
  } while (0)

#define _DBTABLE_ACCESS_WITH_INDEX_EXCEPTION(index)                            \
  do {                                                                         \
    try {                                                                      \
      return it->datum.at(index);                                              \
    } catch (const std::out_of_range &e) {                                     \
      throw TableFieldNotFound(R"(Field index ? out of range.)"_f % (index));  \
    }                                                                          \
  } while (0)

class Table {
public:
  typedef std::string KeyType;
  typedef std::string FieldNameType;
  typedef size_t FieldIndex;
  typedef int ValueType;
  static constexpr const ValueType ValueTypeMax =
      std::numeric_limits<ValueType>::max();
  static constexpr const ValueType ValueTypeMin =
      std::numeric_limits<ValueType>::min();
  typedef size_t SizeType;

private:
  /** A row in the table */
  struct Datum {
    /** Unique key of this datum */
    KeyType key;
    /** The values in the order of fields */
    std::vector<ValueType> datum;

    Datum() = default;

    Datum(const Datum &) = default;

    Datum(Datum &&) noexcept = default;

    Datum &operator=(const Datum &) = default;

    Datum &operator=(Datum &&) noexcept = default;

    ~Datum() = default;

    explicit Datum(const SizeType &size) {
      datum = std::vector<ValueType>(size, ValueType());
    }

    template <class ValueTypeContainer>
    explicit Datum(const KeyType &key, const ValueTypeContainer &datum) {
      this->key = key;
      this->datum = datum;
    }

    explicit Datum(const KeyType &key,
                   std::vector<ValueType> &&datum) noexcept {
      this->key = key;
      this->datum = std::move(datum);
    }
  };

  typedef std::vector<Datum>::iterator DataIterator;
  typedef std::vector<Datum>::const_iterator ConstDataIterator;

  /** The fields, ordered as defined in fieldMap */
  std::vector<FieldNameType> fields;
  /** Map field name into index */
  std::unordered_map<FieldNameType, FieldIndex> fieldMap;

  /** The rows are saved in a vector, which is unsorted */
  std::vector<Datum> data;
  /** Used to keep the keys unique and provide O(1) access with key */
  std::unordered_map<KeyType, SizeType> keyMap;

  /** The name of table */
  std::string tableName;

public:
  typedef std::unique_ptr<Table> Ptr;

  /**
   * A proxy class that provides abstraction on internal Implementation.
   * Allows independent variation on the representation for a table object
   *
   * @tparam Iterator
   * @tparam VType
   */
  template <class Iterator, class VType> class ObjectImpl {
    friend class Table;

    /** Not const because key can be updated */
    Iterator it;
    Table *table;

  public:
    typedef std::unique_ptr<ObjectImpl> Ptr;

    ObjectImpl(Iterator datumIt, const Table *t)
        : it(datumIt), table(const_cast<Table *>(t)) {}

    ObjectImpl(const ObjectImpl &) = default;

    ObjectImpl(ObjectImpl &&) noexcept = default;

    ObjectImpl &operator=(const ObjectImpl &) = default;

    ObjectImpl &operator=(ObjectImpl &&) noexcept = default;

    ~ObjectImpl() = default;

    KeyType key() const { return it->key; }

    void setKey(KeyType key) {
      auto keyMapIt = table->keyMap.find(it->key);
      auto dataIt = std::move(keyMapIt->second);
      table->keyMap.erase(keyMapIt);
      table->keyMap.emplace(key, std::move(dataIt));
      it->key = std::move(key);
    }

    /**
     * Accessing by index should be, at least as fast as accessing by field
     * name. Clients should prefer accessing by index if the same field is
     * accessed frequently (the implement is improved so that index is actually
     * faster now)
     */
    VType &operator[](const FieldNameType &field) const {
      _DBTABLE_ACCESS_WITH_NAME_EXCEPTION(field);
    }

    VType &operator[](const FieldIndex &index) const {
      _DBTABLE_ACCESS_WITH_INDEX_EXCEPTION(index);
    }

    VType &get(const FieldNameType &field) const {
      _DBTABLE_ACCESS_WITH_NAME_EXCEPTION(field);
    }

    VType &get(const FieldIndex &index) const {
      _DBTABLE_ACCESS_WITH_INDEX_EXCEPTION(index);
    }
  };

  typedef ObjectImpl<DataIterator, ValueType> Object;
  typedef ObjectImpl<ConstDataIterator, const ValueType> ConstObject;

  /**
   * A proxy class that provides iteration on the table
   * @tparam ObjType
   * @tparam DatumIterator
   */
  template <typename ObjType, typename DatumIterator> class IteratorImpl {
    using difference_type = std::ptrdiff_t;
    using value_type = ObjType;
    using pointer = typename ObjType::Ptr;
    using reference = ObjType;
    using iterator_category = std::random_access_iterator_tag;
    // See https://stackoverflow.com/questions/37031805/

    friend class Table;

    DatumIterator it;
    const Table *table = nullptr;

  public:
    IteratorImpl(DatumIterator datumIt, const Table *t)
        : it(datumIt), table(t) {}

    IteratorImpl() = default;

    IteratorImpl(const IteratorImpl &) = default;

    IteratorImpl(IteratorImpl &&) noexcept = default;

    IteratorImpl &operator=(const IteratorImpl &) = default;

    IteratorImpl &operator=(IteratorImpl &&) noexcept = default;

    ~IteratorImpl() = default;

    pointer operator->() { return createProxy(it, table); }

    reference operator*() { return *createProxy(it, table); }

    IteratorImpl operator+(int n) { return IteratorImpl(it + n, table); }

    IteratorImpl operator-(int n) { return IteratorImpl(it - n, table); }

    IteratorImpl &operator+=(int n) { return it += n, *this; }

    IteratorImpl &operator-=(int n) { return it -= n, *this; }

    IteratorImpl &operator++() { return ++it, *this; }

    IteratorImpl &operator--() { return --it, *this; }

    const IteratorImpl operator++(int) {
      auto retVal = IteratorImpl(*this);
      ++it;
      return retVal;
    }

    const IteratorImpl operator--(int) {
      auto retVal = IteratorImpl(*this);
      --it;
      return retVal;
    }

    bool operator==(const IteratorImpl &other) { return this->it == other.it; }

    bool operator!=(const IteratorImpl &other) { return this->it != other.it; }

    bool operator<=(const IteratorImpl &other) { return this->it <= other.it; }

    bool operator>=(const IteratorImpl &other) { return this->it >= other.it; }

    bool operator<(const IteratorImpl &other) { return this->it < other.it; }

    bool operator>(const IteratorImpl &other) { return this->it > other.it; }
  };

  typedef IteratorImpl<Object, decltype(data.begin())> Iterator;
  typedef IteratorImpl<ConstObject, decltype(data.cbegin())> ConstIterator;
  template <typename ObjType, typename DatumIterator> class DuplicateQueueImpl {
    friend class IteratorImpl<ObjType, DatumIterator>;
    std::vector<SizeType> queue;
    Table *table;

  public:
    DuplicateQueueImpl(Table *t) : table(t) { queue.clear(); }

    DuplicateQueueImpl() = delete;

    DuplicateQueueImpl(const DuplicateQueueImpl &) = default;

    DuplicateQueueImpl(DuplicateQueueImpl &&) noexcept = default;

    DuplicateQueueImpl &operator=(const DuplicateQueueImpl &) = default;

    DuplicateQueueImpl &operator=(DuplicateQueueImpl &&) noexcept = default;

    ~DuplicateQueueImpl() = default;

  private:
    /**
     * Duplicate raws of data by indexes, must from small to big.
     * TODO wznmickey : have possible to improve
     * Now is only basic one. Use the special order may help to improve.
     * @tparam ValueTypeContainer
     * @param index
     */
    size_t duplicateByIndex(const std::vector<SizeType> &index) {
      size_t noCopy = 0;
      if (index.empty())
        return 0;
      (table->data).reserve((table->data).size() + index.size());
      for (const auto i : index) {
        auto newKey = (table->data[i]).key + "_copy";
        if (table->keyMap.find(newKey) !=
            table->keyMap.end()) // the new key exist
        {
          noCopy++;
          continue;
        }
        table->keyMap.emplace(newKey, table->data.size());
        table->data.emplace_back(newKey, (table->data[i]).datum);
      }
      return noCopy;
    }

  public:
    void push(Iterator it) {
      queue.emplace_back(std::distance(it->table->data.begin(), it.it));
    }
    size_t doDuplicate() {
      size_t temp = duplicateByIndex(queue);
      return queue.size() - temp;
    }
  };
  typedef DuplicateQueueImpl<ConstObject, decltype(data.cbegin())>
      DuplicateQueue;
  template <typename ObjType, typename DatumIterator> class DeleteQueueImpl {
    friend class IteratorImpl<ObjType, DatumIterator>;
    std::vector<SizeType> queue;
    Table *table;
    std::atomic_size_t x;

  public:
    DeleteQueueImpl(Table *t, size_t size) : table(t) {
      queue.resize(size);
      x = 0;
    }

    DeleteQueueImpl() = delete;

    DeleteQueueImpl(const DeleteQueueImpl &) = default;

    DeleteQueueImpl(DeleteQueueImpl &&) noexcept = default;

    DeleteQueueImpl &operator=(const DeleteQueueImpl &) = default;

    DeleteQueueImpl &operator=(DeleteQueueImpl &&) noexcept = default;

    ~DeleteQueueImpl() = default;

  private:
    /**
     * Delete a raw of data by its index
     * @tparam ValueTypeContainer
     * @param index
     */
    void deleteByIndex(const SizeType &index) {
      if (index >= (table->data).size()) {
        throw "Wrong Index.";
      }
      if (index + 1 == (table->data).size()) // no need to swap
      {
        table->keyMap.erase((table->data)[index].key);
        table->data.pop_back();
      } else {
        (table->keyMap[(table->data).back().key]) = index;
        std::swap(table->data[index], (table->data).back());
        table->keyMap.erase((table->data).back().key);
        table->data.pop_back();
      }
      return;
    }

    /**
     * Delete raws of data by indexes, must from small to big.
     * TODO wznmickey : have possible to improve
     * Now is only basic one. Use the special order may help to improve.
     * @tparam ValueTypeContainer
     * @param index
     */
    void deleteByIndex(const std::vector<SizeType> &index) {
      if (index.empty())
        return;
      // index.size() must > 0 in the folowing cases
      if (index.size() == (table->data).size()) {
        table->clear();
        return;
      } // if size the same, directly clear. In the following, there is at least
        // one element in the reamin table.
      decltype(index.size()) left = 0;
      auto right = index.size() - 1; // size can not be zero
      while (true) {
        if (left > right)
          return;
        while (left <= right && index[right] == (table->data).size() - 1) {
          table->keyMap.erase(table->data.back().key);
          table->data.pop_back();
          if (right == 0)
            return;
          right--; // must have at least one element not to be deleted.
        }
        if (left > right)
          return;
        (table->keyMap[(table->data).back().key]) = index[left];
        std::swap(table->data[index[left]], table->data.back());
        table->keyMap.erase((table->data).back().key);
        table->data.pop_back();
        left++;
      }
    }

  public:
    void push(Iterator it) {
      auto y = x.fetch_add(1);
      queue[y] = (std::distance(it->table->data.begin(), it.it));
    }
    size_t doDelete() {
      deleteByIndex(queue);
      return queue.size();
    }
    void sort() {
      queue.resize(x);
      // TWO way to sort
      if (x >= 4 * table->data.size()) {
        std::vector<bool> temp(table->data.size(), false);
        for (const auto i : queue) {
          temp[i] = true;
        }
        size_t index = 0;
        for (size_t i = 0; i < temp.size(); i++) {
          if (temp[i] == true) {
            queue[index] = i;
            index++;
          }
        }
      } else {
        std::sort(queue.begin(), queue.end());
      }
    }
  };
  typedef DeleteQueueImpl<ConstObject, decltype(data.cbegin())> DeleteQueue;

private:
  static ConstObject::Ptr createProxy(ConstDataIterator it,
                                      const Table *table) {
    return std::make_unique<ConstObject>(it, table);
  }

  static Object::Ptr createProxy(DataIterator it, const Table *table) {
    return std::make_unique<Object>(it, table);
  }

public:
  Table() = delete;

  explicit Table(std::string name) : tableName(std::move(name)) {}

  /**
   * Accept any container that contains string.
   * @tparam FieldIDContainer
   * @param name: the table name (must be unique in the database)
   * @param fields: an iterable container with fields
   */
  template <class FieldIDContainer>
  Table(const std::string &name, const FieldIDContainer &fields);

  /**
   * Copy constructor from another table
   * @param name: the table name (must be unique in the database)
   * @param origin: the original table copied from
   */
  Table(std::string name, const Table &origin)
      : fields(origin.fields), fieldMap(origin.fieldMap), data(origin.data),
        keyMap(origin.keyMap), tableName(std::move(name)) {}

  /**
   * Find the index of a field in the fieldMap
   * @param field
   * @return fieldIndex
   */
  FieldIndex getFieldIndex(const FieldNameType &field) const;

  /**
   * Insert a row of data by its key
   * @tparam ValueTypeContainer
   * @param key
   * @param data
   */
  void insertByIndex(const KeyType &key, std::vector<ValueType> &&data);

  /**
   * Access the value according to the key
   * @param key
   * @return the Object that KEY = key, or nullptr if key doesn't exist
   */
  Object::Ptr operator[](const KeyType &key);

  /**
   * Set the name of the table
   * @param name
   */
  void setName(std::string name) { this->tableName = std::move(name); }

  /**
   * Get the name of the table
   * @return
   */
  const std::string &name() const { return this->tableName; }

  /**
   * Return whether the table is empty
   * @return
   */
  bool empty() const { return this->data.empty(); }

  /**
   * Return the num of data stored in the table
   * @return
   */
  size_t size() const { return this->data.size(); }

  /**
   * Return the fields in the table
   * @return
   */
  const std::vector<FieldNameType> &field() const { return this->fields; }

  /**
   * Clear all content in the table
   * @return rows affected
   */
  size_t clear() {
    auto result = keyMap.size();
    data.clear();
    keyMap.clear();
    return result;
  }
  void reserve(size_t n)
  {
    this->data.reserve(n);
    this->keyMap.reserve(n);
    return;
  }
  /**
   * Get a begin iterator similar to the standard iterator
   * @return begin iterator
   */
  Iterator begin() { return {data.begin(), this}; }

  /**
   * Get a end iterator similar to the standard iterator
   * @return end iterator
   */
  Iterator end() { return {data.end(), this}; }

  /**
   * Get a const begin iterator similar to the standard iterator
   * @return const begin iterator
   */
  ConstIterator begin() const { return {data.cbegin(), this}; }

  /**
   * Get a const end iterator similar to the standard iterator
   * @return const end iterator
   */
  ConstIterator end() const { return {data.cend(), this}; }

  /**
   * Overload the << operator for complete print of the table
   * @param os
   * @param table
   * @return the origin ostream
   */
  friend std::ostream &operator<<(std::ostream &os, const Table &table);
};

std::ostream &operator<<(std::ostream &os, const Table &table);

template <class FieldIDContainer>
Table::Table(const std::string &name, const FieldIDContainer &fields)
    : fields(fields.cbegin(), fields.cend()), tableName(name) {
  SizeType i = 0;
  for (const auto &field : fields) {
    if (field == "KEY")
      throw MultipleKey("Error creating table \"" + name +
                        "\": Multiple KEY field.");
    fieldMap.emplace(field, i++);
  }
}

#endif // PROJECT_DB_TABLE_H
