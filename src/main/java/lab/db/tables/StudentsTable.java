package lab.db.tables;

 import java.sql.Connection;
 import java.sql.PreparedStatement;
 import java.sql.ResultSet;
 import java.sql.Statement;
 import java.sql.SQLException;
 import java.sql.SQLIntegrityConstraintViolationException;
 import java.util.ArrayList;
 import java.util.Date;
 import java.util.List;
 import java.util.Objects;
 import java.util.Optional;

 import lab.utils.Utils;
 import lab.db.Table;
 import lab.model.Student;

 public final class StudentsTable implements Table<Student, Integer> {    
     public static final String TABLE_NAME = "students";

     private final Connection connection; 

     public StudentsTable(final Connection connection) {
         this.connection = Objects.requireNonNull(connection);
     }

     @Override
     public String getTableName() {
         return TABLE_NAME;
     }

     @Override
     public boolean createTable() {
         // 1. Create the statement from the open connection inside a try-with-resources
         try (final Statement statement = this.connection.createStatement()) {
             // 2. Execute the statement with the given query
             statement.executeUpdate(
                 "CREATE TABLE " + TABLE_NAME + " (" +
                         "id INT NOT NULL PRIMARY KEY," +
                         "firstName CHAR(40)," + 
                         "lastName CHAR(40)," + 
                         "birthday DATE" + 
                     ")");
             return true;
         } catch (final SQLException e) {
             // 3. Handle possible SQLExceptions
             return false;
         }
     }

     @Override
     public Optional<Student> findByPrimaryKey(final Integer id) {
    	 final String query = "SELECT * FROM " + TABLE_NAME + " WHERE id=?";
         try(final PreparedStatement statement = this.connection.prepareStatement(query)){
        	 statement.setInt(1, id);
        	 
        	 final ResultSet resultSet = statement.executeQuery();
        	 return readStudentsFromResultSet(resultSet).stream().findFirst();
         } catch(final SQLException e) {
        	 return Optional.empty();
         }
     }

     /**
      * Given a ResultSet read all the students in it and collects them in a List
      * @param resultSet a ResultSet from which the Student(s) will be extracted
      * @return a List of all the students in the ResultSet
      */
     private List<Student> readStudentsFromResultSet(final ResultSet resultSet) {
         // Create an empty list, then
         // Inside a loop you should:
         //      1. Call resultSet.next() to advance the pointer and check there are still rows to fetch
         //      2. Use the getter methods to get the value of the columns
         //      3. After retrieving all the data create a Student object
         //      4. Put the student in the List
         // Then return the list with all the found students

         // Helpful resources:
         // https://docs.oracle.com/javase/7/docs/api/java/sql/ResultSet.html
         // https://docs.oracle.com/javase/tutorial/jdbc/basics/retrieving.html
    	 
    	 List<Student> allStudents = new ArrayList<>();
    	 try {
			while(resultSet.next()) {
				allStudents.add(new Student(resultSet.getInt("id"), resultSet.getString("firstName"), resultSet.getString("lastName"), resultSet.getDate("birthday") == null ? Optional.empty() : Optional.of(resultSet.getDate("birthday"))));
			 }
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
    	 
         return allStudents;
     }

     @Override
     public List<Student> findAll() {
         try (final Statement statement = this.connection.createStatement()) {
             // 2. Execute the statement with the given query
             final ResultSet resultSet = statement.executeQuery("SELECT * FROM " + TABLE_NAME);
             return this.readStudentsFromResultSet(resultSet);
         } catch (final SQLException e) {
             // 3. Handle possible SQLExceptions
             return new ArrayList<Student>();
         }
     }

     public List<Student> findByBirthday(final Date date) {
    	 List<Student> students = new ArrayList<>();
    	 final String query = "SELECT * FROM " + TABLE_NAME + " WHERE birthday=?";
         try(final PreparedStatement statement = this.connection.prepareStatement(query)){
        	 statement.setDate(1, Utils.dateToSqlDate(date));
        	 
        	 final ResultSet resultSet = statement.executeQuery();
        	 while(resultSet.next()) {
 				students.add(new Student(resultSet.getInt("id"), resultSet.getString("firstName"), resultSet.getString("lastName"), resultSet.getDate("birthday") == null ? Optional.empty() : Optional.of(resultSet.getDate("birthday"))));
        	 }
        	 return students;
         } catch(final SQLException e) {
        	 return new ArrayList<Student>();
         }

     }

     @Override
     public boolean dropTable() {
         try (final Statement statement = this.connection.createStatement()) {
             // 2. Execute the statement with the given query
             statement.executeUpdate("DROP TABLE " + TABLE_NAME);
             return true;
         } catch (final SQLException e) {
        	 //e.printStackTrace();
             // 3. Handle possible SQLExceptions
             return false;
         }
     }

     @Override
     public boolean save(final Student student) {
    	 final String query = "INSERT INTO students (id, firstName, lastName, birthday) VALUES (?, ?, ?, ?) ";
         try(final PreparedStatement statement = this.connection.prepareStatement(query)){
        	 statement.setInt(1, student.getId());
        	 statement.setString(2, student.getFirstName());
        	 statement.setString(3, student.getLastName());
        	 statement.setDate(4, student.getBirthday().isPresent() ? Utils.dateToSqlDate(student.getBirthday().get()) : null);
        	 final int resultSet = statement.executeUpdate();
        	 return true;
         } catch(final SQLException e) {
        	 //e.printStackTrace();
        	 return false;
         }

     }

     @Override
     public boolean delete(final Integer id) {
    	 final String query = "DELETE FROM students WHERE id=? ";
         try(final PreparedStatement statement = this.connection.prepareStatement(query)){
        	 statement.setInt(1, id);
        	 final int resultSet = statement.executeUpdate();
        	 return resultSet > 0;
         } catch(final SQLException e) {
        	 e.printStackTrace();
        	 return false;
         }

     }

     @Override
     public boolean update(final Student student) {
    	 final String query = "UPDATE " + TABLE_NAME + " SET firstName=?, lastName=?, birthday=? WHERE id=?";
         try(final PreparedStatement statement = this.connection.prepareStatement(query)){
        	 statement.setString(1, student.getFirstName());
        	 statement.setString(2, student.getLastName());
        	 statement.setDate(3, student.getBirthday().isPresent() ? Utils.dateToSqlDate(student.getBirthday().get()) : null);
        	 statement.setInt(4, student.getId());
        	 final int resultSet = statement.executeUpdate();
        	 return resultSet > 0;
         } catch(final SQLException e) {
        	 e.printStackTrace();
        	 return false;
         }

     }
 }