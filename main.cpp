#include <iostream>
#include <vector>
#include <sstream>
#include <fstream>
#include <string.h>
#include <mpi.h>
#include <math.h>

using namespace std;

int world_size,world_rank, aux_mpi=1;

string to_string(int n)
{
    ostringstream convert;
    convert<<n;
    return convert.str();
}

vector<string> split(const string& s, char delimiter)
{
    vector<string> tokens;
    string token;
    istringstream tokenStream(s);
    while (getline(tokenStream, token, delimiter))
    {
        tokens.push_back(token);
    }
    return tokens;
}

void join_file()
{
    string file=to_string(world_rank)+".txt";
    char* f = new char [file.length()+1];
    strcpy (f, file.c_str());
    ofstream fs(file.c_str(), ios::trunc);
    for(int i = 1 ; i < world_size ; i++)
    {
        string cadena;
        file=to_string(i)+".txt";
        f = new char [file.length()+1];
        strcpy (f, file.c_str());
        ifstream fe(f);
        while(getline (fe,cadena))
            fs<<cadena<<"\n";
        fe.close();
        remove(f);
    }
    fs.close();
}

void distinct()
{
    vector<string> carreras, carreras_distintas;
    string cadena, file=to_string(world_rank)+".txt";
    char* f = new char [file.length()+1];
    strcpy (f, file.c_str());
    ifstream fe(f);
    while(getline (fe,cadena))
        carreras.push_back(cadena);
    fe.close();
    while(!carreras.empty())
    {
        string carrera = carreras.back();
        carreras.pop_back();
        if(carreras_distintas.empty())
            carreras_distintas.push_back(carrera);
        else
        {
            bool agregar=true;
            for(int i = 0 ; i < carreras_distintas.size() ; i++)
                if(carreras_distintas.at(i)==carrera)
                {
                    agregar=false;
                    break;
                }
            if(agregar)
                carreras_distintas.push_back(carrera);

        }
    }
    carreras_distintas.erase(carreras_distintas.begin());
    file=to_string(world_rank)+".txt";
    f = new char [file.length()+1];
    strcpy (f, file.c_str());
    ofstream fs(file.c_str(), ios::trunc);
    for(int i=0;i<carreras_distintas.size();i++)
            fs<<carreras_distintas.at(i)<<"\n";
    fs.close();
}

void split_file()
{
    vector<string> carreras;
    string cadena, file="utem.csv";
    char* f = new char [file.length()+1];
    strcpy (f, file.c_str());
    ifstream fe(f);
    while(getline (fe,cadena))
        carreras.push_back(split(cadena,',')[1]);
    fe.close();
    int it=(world_size<=carreras.size())?world_size:carreras.size();
    for(int i = 1 , contador=carreras.size()-1; i < it ; i++)
    {
        file=to_string(i)+".txt";
        f = new char [file.length()+1];
        strcpy (f, file.c_str());
        ofstream fs(file.c_str(), ios::trunc);
        int it2 = round((float)carreras.size()/it);
        int final_size=(i<it-1)?it2:(carreras.size()-((i-1)*it2));
        for(int j=0;j<final_size;j++,contador--)
                fs<<carreras.at(contador)<<"\n";
        fs.close();
        MPI_Send(&aux_mpi, 1, MPI_INT, i, 0, MPI_COMM_WORLD);
    }
}

int main(int argc, char* argv[])
{
    MPI_Init(&argc,&argv);
    MPI_Comm_size(MPI_COMM_WORLD, &world_size);
    MPI_Comm_rank(MPI_COMM_WORLD, &world_rank);
    if(!world_rank)
    {
        split_file();
        for(int i = 1, dummy ; i < world_size ; i++)
            MPI_Recv(&dummy, 1, MPI_INT, i, 0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        join_file();
        distinct();
    }
    else
    {
        int dummy;
        MPI_Recv(&dummy, 1, MPI_INT, 0, 0, MPI_COMM_WORLD,MPI_STATUS_IGNORE);
        distinct();
        MPI_Send(&aux_mpi, 1, MPI_INT, 0, 0, MPI_COMM_WORLD);
    }

    return 0;
}
