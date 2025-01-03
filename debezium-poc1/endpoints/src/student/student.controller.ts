import { Body, Controller, Post } from '@nestjs/common';
import { StudentService } from './student.service';
import { Student } from 'src/student.entity';

@Controller('students')
export class StudentController {
  constructor(private readonly studentService: StudentService) {}

  @Post()
  async create(@Body() createStudentDto: Partial<Student>): Promise<Student> {
    return this.studentService.createStudent(createStudentDto);
  }

}
